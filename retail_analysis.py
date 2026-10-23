
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



# ==============================
# DATA VISUALIZATION
# ==============================

import os
import matplotlib.pyplot as plt

viz_dir = r"C:\PySparkProject\visualizations"
os.makedirs(viz_dir, exist_ok=True)

# 1. Monthly Revenue Trend
monthly_pd = clean_df.withColumn(
    "Month", date_format(col("InvoiceDate"), "yyyy-MM")
).groupBy("Month").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy("Month").toPandas()

plt.figure(figsize=(12, 6))
plt.plot(
    monthly_pd["Month"],
    monthly_pd["Total Revenue"],
    marker="o"
)
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue (£)")
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(
    os.path.join(viz_dir, "monthly_revenue.png"),
    dpi=200
)
plt.close()

# 2. Top 10 Countries by Revenue
countries_pd = clean_df.groupBy("Country").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy(col("Total Revenue").desc()).limit(10).toPandas()

countries_pd = countries_pd.sort_values("Total Revenue")

plt.figure(figsize=(10, 6))
plt.barh(
    countries_pd["Country"],
    countries_pd["Total Revenue"]
)
plt.title("Top 10 Countries by Revenue")
plt.xlabel("Revenue (£)")
plt.ylabel("Country")
plt.tight_layout()
plt.savefig(
    os.path.join(viz_dir, "top_countries.png"),
    dpi=200
)
plt.close()

# 3. Top 10 Products by Revenue
products_pd = clean_df.groupBy("Description").agg(
    round(sum("Revenue"), 2).alias("Total Revenue")
).orderBy(col("Total Revenue").desc()).limit(10).toPandas()

products_pd = products_pd.sort_values("Total Revenue")

plt.figure(figsize=(12, 7))
plt.barh(
    products_pd["Description"],
    products_pd["Total Revenue"]
)
plt.title("Top 10 Products by Revenue")
plt.xlabel("Revenue (£)")
plt.ylabel("Product")
plt.tight_layout()
plt.savefig(
    os.path.join(viz_dir, "top_products.png"),
    dpi=200
)
plt.close()

print("Visualizations saved successfully:", viz_dir)


spark.stop()
