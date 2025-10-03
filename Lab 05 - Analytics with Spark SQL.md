# **Lab 05 — Analytics with Spark SQL**

---

## Description

You are working for an **e-commerce company** that stores **customer data in CSV files** and **order data in JSON logs**. The marketing team wants to analyze **customer spending across countries and categories** to identify high-value customers and product demand.

You will use **Spark SQL** to clean, join, and analyze these datasets.

---



## **Step 1: Create Spark Session**

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Customer-Sales-Analytics") \
    .master("local[*]") \
    .getOrCreate()
```
---
## **Step 2: Load Datasets**

```python
customers_df = spark.read.option("header", True).csv("customers.csv")
orders_df = spark.read.json("orders.jsonl")

customers_df.show(5)
orders_df.show(5)
```
---
## **Step 3: Validate Data (Nulls, Duplicates)**

```python
from pyspark.sql import functions as F

customers_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in customers_df.columns]).show()
orders_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in orders_df.columns]).show()
```
---
## **Step 4: Register SQL Views**

```python
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")
```
---
## **Step 5: Run SQL Queries**

* **Top Customers by Spending**

```python
top_customers = spark.sql("""
    SELECT c.name, c.country, SUM(o.amount) AS total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.name, c.country
    ORDER BY total_spent DESC
    LIMIT 10
""")
top_customers.show()
```

* **Revenue by Category per Country**

```python
category_stats = spark.sql("""
    SELECT c.country, o.category, ROUND(SUM(o.amount), 2) AS total_sales
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.country, o.category
    ORDER BY total_sales DESC
""")
category_stats.show()
```
---
## **Step 6: Save Results to Parquet**

```python
category_stats.write.mode("overwrite").parquet("output/sales_by_category")
```
---

## **Step 7: Export to CSV for BI**

```python
category_stats.write.mode("overwrite").csv("output/sales_by_category_csv", header=True)
```

---

##  Outcome

* You generated **insights on top customers and sales per country/category**.
* Analysts can now **load the CSV into Tableau/Power BI** for visualization.

---

