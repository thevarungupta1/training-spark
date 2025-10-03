## **Lab 5 — Building Machine Learning Models with PySpark MLlib**

##  Description

In this lab, you will explore **PySpark MLlib**, Spark’s machine learning library.
You will prepare features, split data into training and testing sets, train a **Logistic Regression model**, and evaluate its performance.

This lab introduces how PySpark enables **scalable machine learning** on large datasets using distributed computing.

##  Objectives

By the end of this lab, you will be able to:

* Load and preprocess data for machine learning.
* Perform **feature engineering** using `VectorAssembler`.
* Train a **classification model** with Logistic Regression.
* Evaluate the model’s performance using metrics such as AUC.


##  Dataset

`customer_churn.csv`

```csv
customer_id,age,income,transactions,churn
1,28,50000,5,0
2,35,60000,15,1
3,40,65000,20,1
4,22,30000,2,0
5,30,70000,25,1
6,45,80000,18,0
7,29,40000,10,0
8,50,90000,30,1
```

---


### **Step 1: Install & Initialize Spark**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("lab3-ml") \
    .master("local[*]") \
    .getOrCreate()
```

### **Step 2: Load Data**

```python
df = spark.read.option("header", True).option("inferSchema", True).csv("customer_churn.csv")
df.show()
```

### **Step 3: Feature Engineering**

```python
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["age", "income", "transactions"],
    outputCol="features"
)

final_df = assembler.transform(df).select("features", "churn")
final_df.show()
```

### **Step 4: Train-Test Split**

```python
train_df, test_df = final_df.randomSplit([0.7, 0.3], seed=42)
```

### **Step 5: Train Logistic Regression Model**

```python
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(labelCol="churn", featuresCol="features")
model = lr.fit(train_df)
```

### **Step 6: Evaluate Model**

```python
from pyspark.ml.evaluation import BinaryClassificationEvaluator

predictions = model.transform(test_df)
evaluator = BinaryClassificationEvaluator(labelCol="churn")
accuracy = evaluator.evaluate(predictions)

print("Model Evaluation (AUC):", accuracy)
```

 *Outcome:* You built and evaluated a **classification model** in PySpark MLlib.

---


