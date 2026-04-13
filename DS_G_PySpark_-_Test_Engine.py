import findspark
findspark.init()

from pyspark.sql import SparkSession

# Initialize Spark Session
# We set shuffle partitions to 2 because we are on a laptop, not a cluster
spark = SparkSession.builder \
    .appName("DE_Project_Gold_Layer") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Create a small dataset to test
data = [("CPU_High", 95.5), ("Memory_OK", 40.2), ("Battery_Low", 15.0)]
columns = ["Status", "Value_Pct"]

df = spark.createDataFrame(data, columns)

print("\n🚀 Spark is Alive! Here is your Dataframe:")
df.show()

# Stop the session
spark.stop()


# import os
# import sys
# import findspark

# # Use the Java 17 Short Path
# os.environ['JAVA_HOME'] = r'C:\PROGRA~1\Eclipse Adoptium\jdk-17.0.18.8-hotspot'
# os.environ['SPARK_HOME'] = r'C:\spark'
# os.environ['HADOOP_HOME'] = r'C:\hadoop'
# os.environ['PYSPARK_PYTHON'] = sys.executable

# findspark.init()

# from pyspark.sql import SparkSession

# print("Final attempt to wake the Lion with Java 17...")

# try:
#     spark = SparkSession.builder \
#         .master("local[1]") \
#         .appName("Java17Success") \
#         .config("spark.driver.host", "127.0.0.1") \
#         .getOrCreate()

#     print("\n✅ SUCCESS! PROJECT INFRASTRUCTURE COMPLETE.")
#     spark.createDataFrame([("Java 17", "Stable"), ("Spark 3.5", "Running")], ["Tool", "Status"]).show()

# except Exception as e:
#     print(f"❌ Still failing. Error: {e}")

# finally:
#     if 'spark' in locals():
#         spark.stop()