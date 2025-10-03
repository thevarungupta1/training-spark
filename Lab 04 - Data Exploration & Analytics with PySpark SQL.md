# **Lab 04 — Data Exploration & Analytics with PySpark SQL**

## Description

In this lab, you will learn how to use **PySpark SQL** for analytical queries on large datasets.
You will register DataFrames as SQL tables, run queries to filter, aggregate, and group data, and compare SQL queries with DataFrame API operations.

This lab demonstrates how PySpark brings the **power of SQL and distributed computing** together for data analytics.

## Objectives

By the end of this lab, you will be able to:

* Register DataFrames as **temporary SQL tables**.
* Write and execute **SQL queries** on big data.
* Perform **grouping and aggregations** to extract insights.
* Save query results for reporting and further analysis.

---

## Dataset

(Same as Lab 1: `customers.csv` + `orders.jsonl`)

---


### **Step 1: Install & Initialize Spark**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("lab2-sql") \
    .master("local[*]") \
    .getOrCreate()
```

### **Step 2: Load Data**

```python
customers_df = spark.read.option("header", True).csv("customers.csv")
orders_df = spark.read.json("orders.jsonl")
```

### **Step 3: Register as SQL Tables**

```python
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")
```

### **Step 4: Run SQL Queries**

```python
# Total spending by each customer
result = spark.sql("""
    SELECT c.name, o.category, SUM(o.amount) as total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.name, o.category
    ORDER BY total_spent DESC
""")

result.show()
```

### **Step 5: Save Results**

```python
result.write.mode("overwrite").csv("processed/sql_results", header=True)
```

*Outcome:* You used **SQL queries** on big data with PySpark.

---
