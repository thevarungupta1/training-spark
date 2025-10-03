# **Lab 03 — Data Processing Fundamentals with PySpark**

## Description

In this lab, you will learn the fundamentals of **PySpark DataFrames** and distributed data processing.
You will practice loading structured and semi-structured data, exploring schemas, applying transformations, and saving results in optimized formats.

This lab introduces how PySpark simplifies **ETL (Extract, Transform, Load)** tasks on large datasets.

##  Objectives

By the end of this lab, you will be able to:

* Initialize a **SparkSession** in Python.
* Load data from **CSV** and **JSON** sources.
* Explore and understand data schemas.
* Apply basic **transformations** (filtering, selecting, joining).
* Save transformed results into a **processed dataset**.

---

### **Step 1: Install and Configure PySpark**

1. Create a Python virtual environment (optional but recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   ```
2. Install dependencies:

   ```bash
   pip install pyspark findspark jupyter
   ```

---

### **Step 2: Start Jupyter Notebook**

Run:

```bash
jupyter notebook
```

Open `01_pyspark_lab.ipynb`.

---

### **Step 3: Initialize Spark Session**

In your notebook, add:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("pyspark-lab")
         .master("local[*]")
         .getOrCreate())
```

---

### **Step 4: Load Sample Data**

Create sample data files

`customers.csv`
```csv
customer_id,name,age,city
1,John,28,New York
2,Alice,34,London
3,Bob,25,Sydney
4,Emma,30,Delhi
5,David,40,San Francisco

```

`orders.json`
```json
{"order_id": 101, "customer_id": 1, "amount": 250, "category": "Electronics"}
{"order_id": 102, "customer_id": 2, "amount": 450, "category": "Clothing"}
{"order_id": 103, "customer_id": 1, "amount": 120, "category": "Books"}
{"order_id": 104, "customer_id": 3, "amount": 300, "category": "Sports"}
{"order_id": 105, "customer_id": 4, "amount": 500, "category": "Electronics"}

```

Use provided files (`customers.csv`, `orders.jsonl`).

```python
customers_df = spark.read.option("header", True).csv("customers.csv")
orders_df = spark.read.json("orders.json")

customers_df.show()
orders_df.show()

```

---

### **Step 5: Explore Data with DataFrame API**

1. Print schema:

   ```python
   customers_df.printSchema()
   orders_df.printSchema()
   ```
2. Select & filter:

   ```python
   customers_df.select("customer_id", "name").show()
   orders_df.filter(orders_df["amount"] > 100).show()
   ```

---

### **Step 6: Perform Transformations**

Join customers with orders:

```python
# Filter customers older than 30
customers_df.filter(customers_df.age > 30).show()

# Join customers and orders
joined_df = customers_df.join(orders_df, "customer_id", "inner")
joined_df.show()

```

---

### **Step 7: Save Processed Data**

Write results to processed folder:

```python
joined_df.write.mode("overwrite").parquet("processed/orders_summary")
```

---

## Expected Outcome

* You successfully set up **PySpark** in a Jupyter Notebook.
* You loaded **CSV** and **JSON** data, explored it, and performed transformations.
* You saved the transformed data back as **Parquet**.

---
