import sys
import os
import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 1. Setup
os.environ['JAVA_HOME'] = r'C:\PROGRA~1\Eclipse Adoptium\jdk-17.0.18.8-hotspot'
os.environ['SPARK_HOME'] = r'C:\spark'
findspark.init()

spark = SparkSession.builder \
    .appName("ActualLaptopMetrics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    # 2. READ: Point this to your folder created by the Parquet Consumer
    # Note: Spark can read the whole folder at once!
    print("Reading real Parquet data...")
    df = spark.read.parquet("parquet_data") 

    # 3. ANALYZE: Let's see your average CPU and RAM
    # Change column names below to match what you used in your Producer!
    print("\n🏆 GOLD LAYER AGGREGATIONS:")
    df.select(
        F.avg("cpu_usage_pct").alias("Average_CPU_%"),
        F.max("mem_usage_pct").alias("Peak_RAM_Usage"),
        F.count("*").alias("Total_Samples_Collected")
    ).show()

except Exception as e:
    print(f"Error reading Parquet: {e}")
    print("Check if 'parquet_data' folder exists and has .parquet files inside.")

finally:
    spark.stop()
    sys.exit(0)