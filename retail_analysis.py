
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum, round, countDistinct, date_format

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("OnlineRetailSalesAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

file_path = r"C:\PySparkProject\OnlineRetail.csv"

# Load data
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Clean data
df = df.withColumn(
    "InvoiceDate",
    to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
)

clean_df = df.dropDuplicates().filter(
    col("Description").isNotNull() &
    col("InvoiceDate").isNotNull() &
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0)
)

# Calculate revenue
clean_df = clean_df.withColumn(
    "Revenue",
    round(col("Quantity") * col("UnitPrice"), 2)
)

# 1. Overall sales summary
print("\n--- OVERALL SALES SUMMARY ---")

clean_df.agg(
    round(sum("Revenue"), 2).alias("Total Revenue"),
    countDistinct("InvoiceNo").alias("Total Invoices"),
    countDistinct("CustomerID").alias("Unique Customers")
).show(truncate=False)

# 2. Top 10 products by revenue
print("\n--- TOP 10 PRODUCTS BY REVENUE ---")

clean_df.groupBy("Description").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy(col("Total Revenue").desc()).show(10, truncate=False)

# 3. Top 10 countries by revenue
print("\n--- TOP 10 COUNTRIES BY REVENUE ---")

clean_df.groupBy("Country").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy(col("Total Revenue").desc()).show(10, truncate=False)

# 4. Monthly sales trend
print("\n--- MONTHLY SALES TREND ---")

clean_df.withColumn(
    "Month", date_format(col("InvoiceDate"), "yyyy-MM")
).groupBy("Month").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy("Month").show(20, truncate=False)


import os

output_dir = r"C:\PySparkProject\output"
os.makedirs(output_dir, exist_ok=True)

# Export overall sales summary
clean_df.agg(
    round(sum("Revenue"), 2).alias("Total Revenue"),
    countDistinct("InvoiceNo").alias("Total Invoices"),
    countDistinct("CustomerID").alias("Unique Customers")
).coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(output_dir + r"\overall_summary")

# Export top products
clean_df.groupBy("Description").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy(col("Total Revenue").desc()).limit(10).coalesce(1).write.mode(
    "overwrite"
).option("header", True).csv(output_dir + r"\top_products")

# Export top countries
clean_df.groupBy("Country").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy(col("Total Revenue").desc()).limit(10).coalesce(1).write.mode(
    "overwrite"
).option("header", True).csv(output_dir + r"\top_countries")

# Export monthly trends
clean_df.withColumn(
    "Month", date_format(col("InvoiceDate"), "yyyy-MM")
).groupBy("Month").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy("Month").coalesce(1).write.mode(
    "overwrite"
).option("header", True).csv(output_dir + r"\monthly_sales")

print("All analysis results exported to:", output_dir)


spark.stop()
