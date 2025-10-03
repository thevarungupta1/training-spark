# **Lab 07 — Customer Data Integration with MySQL and PySpark**

---

##  Description

A company keeps **customer master data in MySQL**. You are tasked to build a **PySpark ETL pipeline** that:

* Reads customer data from MySQL.
* Calculates customer distribution by country.
* Writes results back to MySQL for use in dashboards.

---



## **Step 1: Setup MySQL Database**

```sql
CREATE DATABASE sparkdb;
USE sparkdb;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    country VARCHAR(50)
);

INSERT INTO customers VALUES
(1, 'John', 28, 'USA'),
(2, 'Alice', 34, 'UK'),
(3, 'Bob', 25, 'India'),
(4, 'Emma', 30, 'USA'),
(5, 'Raj', 29, 'India');
```

## **Step 2: Create Spark Session with JDBC Connector**

```python
spark = SparkSession.builder \
    .appName("Customer-MySQL-ETL") \
    .master("local[*]") \
    .config("spark.jars", "C:/spark/jars/mysql-connector-java-8.0.33.jar") \
    .getOrCreate()
```

## **Step 3: Read from MySQL**

```python
jdbc_url = "jdbc:mysql://localhost:3306/sparkdb"
properties = {"user": "root", "password": "your_password", "driver": "com.mysql.cj.jdbc.Driver"}

customers_db = spark.read.jdbc(url=jdbc_url, table="customers", properties=properties)
customers_db.show()
```

## **Step 4: Aggregation: Customers by Country**

```python
country_stats = customers_db.groupBy("country").count().orderBy("count", ascending=False)
country_stats.show()
```

## **Step 5: Write Back to MySQL (new table)**

```python
country_stats.write.jdbc(
    url=jdbc_url,
    table="customer_distribution",
    mode="overwrite",
    properties=properties
)
```

## **Step 6: Validate in MySQL**

```sql
SELECT * FROM customer_distribution;
```

---

##  Outcome

* You built an **ETL pipeline** connecting Spark ↔ MySQL.
* Data scientists can now query **aggregated customer stats** in dashboards (Power BI, Tableau).
* This simulates **real-world data engineering workflows** where Spark is the engine between raw data and BI systems.

---

