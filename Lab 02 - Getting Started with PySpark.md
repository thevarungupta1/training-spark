# Lab 02 — Setting up Apache Spark on Local Windows Machine

---

##  Objective

By the end of this lab, you will:

* Install **Java**, **Hadoop (winutils)**, **Spark**, and **Python** dependencies.
* Configure **environment variables** for Spark.
* Run **PySpark shell** and verify the setup.
* Open Spark in a **Jupyter Notebook** for data exploration.

---


### **Step 1: Install Prerequisites**

1. **Install Java (JDK 8 or higher)**

   * Download from: [Oracle JDK](https://www.oracle.com/java/technologies/javase-downloads.html) or [AdoptOpenJDK](https://adoptium.net/).
   * Install it and note the installation path (e.g., `C:\Program Files\Java\jdk-17`).

2. **Install Python (3.8 or higher recommended)**

   * Download from: [Python.org](https://www.python.org/downloads/).
   * Check installation:

     ```bash
     python --version
     pip --version
     ```

3. **Install Hadoop winutils.exe (required for Windows)**

   * Spark on Windows needs a helper binary.
   * Download `winutils.exe` from [GitHub Repo](https://github.com/steveloughran/winutils) (choose version matching Hadoop, e.g., `hadoop-3.2.2`).
   * Place it in:

     ```
     C:\hadoop\bin\winutils.exe
     ```

---

### **Step 2: Install Spark**

1. Download Spark from: [Apache Spark Downloads](https://spark.apache.org/downloads.html).

   * Choose: **Spark 4.x** with **Pre-built for Hadoop 3.2 and later**.

2. Extract Spark to:

   ```
   C:\spark
   ```

3. Verify folder structure:

   ```
   C:\spark\bin
   C:\spark\conf
   C:\spark\jars
   ```

---

### **Step 3: Set Environment Variables**

Open **System Properties → Environment Variables** and add the following:

* **JAVA_HOME**

  ```
  C:\Program Files\Java\jdk-17
  ```
* **SPARK_HOME**

  ```
  C:\spark
  ```
* **HADOOP_HOME**

  ```
  C:\hadoop
  ```
* **PATH** (append):

  ```
  %JAVA_HOME%\bin
  %SPARK_HOME%\bin
  %HADOOP_HOME%\bin
  ```

 Restart your terminal after setting these variables.

---

### **Step 4: Verify Installation**

Open **Command Prompt (cmd)** or **PowerShell** and run:

```bash
java -version
python --version
spark-shell
```

* `spark-shell` should open a Scala-based Spark REPL.
* Close it with `:quit`.

---

### **Step 5: Test PySpark Shell**

Run:

```bash
pyspark
```

Expected output:

* Spark session starts.
* You’ll see:

  ```
  Using Spark's default log4j profile...
  Spark context Web UI available at http://localhost:4040
  SparkSession available as 'spark'.
  ```

Try a quick test:

```python
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()
```

Should display the table with 3 rows.

---

### **Step 6: Integrate with Jupyter Notebook**

1. Install Jupyter & findspark:

   ```bash
   pip install jupyter findspark
   ```

2. Add Spark to Jupyter:

   ```python
   import findspark
   findspark.init()
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("windows-spark-lab").getOrCreate()
   print(spark.version)
   ```

3. Start Jupyter Notebook:

   ```bash
   jupyter notebook
   ```

4. Create a new notebook → Test Spark by running:

   ```python
   df = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
   df.show()
   ```

---

##  Expected Outcome

* You have Spark installed and running on Windows.
* You can launch **pyspark shell** and **run notebooks** with Spark.
* You are ready to build PySpark projects locally.

---
