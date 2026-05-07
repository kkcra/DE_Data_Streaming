import sys
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

#############################################################################

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

#############################################################################

# 1. ENVIRONMENT SETUP
os.environ['JAVA_HOME'] = r'C:\PROGRA~1\Eclipse Adoptium\jdk-17.0.18.8-hotspot'
os.environ['SPARK_HOME'] = r'C:\spark'
findspark.init()

def main():
    # 2. INITIALIZE SESSION
    spark = SparkSession.builder \
            .appName("PySpark_Quickstart") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

    # --- THE MAGIC FIX FOR WINDOWS NativeIO ---
    # This tells Hadoop to stop trying to use the native Windows access check
    spark.conf.set("spark.hadoop.fs.permissions.umask-mode", "000")
    # ------------------------------------------

    # Suppress noise
    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------
    # ------------------------------------------
    # ------------------------------------------

    try:
        df = spark.createDataFrame([
            Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
            Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
            Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
        ])
        df.show()
        print("✅ DataFrame created successfully!")

    # ------------------------------------------

    except Exception as e:
        print(f"❌ Error during processing: {e}")
        print("Hint: Make sure your Consumer has actually saved .parquet files in the directory.")

    # ------------------------------------------

    finally:
        print("🔌 Shutting down Spark...")
        spark.stop()
        sys.exit(0)

    # ------------------------------------------
    # ------------------------------------------
    # ------------------------------------------

if __name__ == "__main__":
    main()