#  Lab 06 — Real-Time Log Monitoring with Spark Streaming

---

##  Description

Your DevOps team needs a **real-time monitoring system** to track errors from application logs. Instead of waiting for daily reports, the team wants to **stream logs live** and see error frequency.

In this lab, you will:

* Simulate a log server using **Python sockets**
* Use **Spark Structured Streaming** to process logs in real-time
* Filter error keywords (`error`, `timeout`, `failed`, `exception`)
* Output live error counts to the console

---

### **Step 1: Create a Log Server (Python)**

Create a file `log_server.py` in your project root:

```python
import socket
import time

HOST = "localhost"
PORT = 9999

# Create TCP server
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)
print(f"Log server started on {HOST}:{PORT}")

conn, addr = server_socket.accept()
print("Connected by", addr)

# Example log messages
logs = [
    "user login success",
    "error database timeout",
    "payment success",
    "error failed connection",
    "user logout success",
    "timeout retry",
    "exception while processing request"
]

# Send logs one by one
for log in logs:
    conn.sendall((log + "\n").encode("utf-8"))
    print("Sent:", log)
    time.sleep(2)  # delay to simulate streaming

conn.close()
```

Run this in a separate terminal:

```bash
python log_server.py
```

---

### **Step 2: Start Spark Streaming Job (PySpark)**

Create a notebook or script `streaming_job.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Create Spark session
spark = SparkSession.builder \
    .appName("RealTime-Log-Monitoring") \
    .master("local[*]") \
    .getOrCreate()

# Read stream from socket
lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Filter error-related logs
errors = words.filter(col("word").isin("error", "failed", "timeout", "exception"))

# Count error keywords
error_counts = errors.groupBy("word").count()

# Output results to console
query = error_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

---

### **Step 3: Run the Lab**

1. Start **log server**:

   ```bash
   python log_server.py
   ```

2. Start **Spark streaming job**:

   ```bash
   python streaming_job.py
   ```

3. Watch the console — you’ll see error counts update every time logs are streamed.

Example output:

```
+---------+-----+
|     word|count|
+---------+-----+
|    error|    2|
|  timeout|    1|
|   failed|    1|
|exception|    1|
+---------+-----+
```

---

### **Step 4: (Optional) Save to File for Reporting**

You can also write results to a folder:

```python
error_counts.writeStream \
    .outputMode("complete") \
    .format("csv") \
    .option("path", "logs/output") \
    .option("checkpointLocation", "logs/checkpoints") \
    .start()
```

Now Spark will save results into `logs/output/` for use in dashboards.

---

##  Outcome

* You simulated a **real-time log server**.
* Spark Streaming consumed the logs, filtered errors, and counted them in real-time.
* Results were **printed live** and optionally saved for **reporting dashboards**.
* This mimics real-world log processing pipelines (Kafka → Spark → BI).

