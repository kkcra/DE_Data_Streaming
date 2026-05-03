import sys
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# 1. ENVIRONMENT SETUP
os.environ['JAVA_HOME'] = r'C:\PROGRA~1\Eclipse Adoptium\jdk-17.0.18.8-hotspot'
os.environ['SPARK_HOME'] = r'C:\spark'
findspark.init()

def main():
    # 2. INITIALIZE SESSION
    spark = SparkSession.builder \
            .appName("LaptopHealth_GoldLayer") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

    # --- THE MAGIC FIX FOR WINDOWS NativeIO ---
    # This tells Hadoop to stop trying to use the native Windows access check
    spark.conf.set("spark.hadoop.fs.permissions.umask-mode", "000")
    # ------------------------------------------

    # Suppress noise
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("🛠️  Step 1: Reading Silver Layer (Parquet)...")
        
        # Spark reads all .parquet files in the folder automatically
        # Replace "parquet_data" with your folder name if different
        silver_df = spark.read.parquet("parquet_data")

        print(f"📈 Total records ingested: {silver_df.count()}")
        silver_df.printSchema() # Shows the structure Spark detected

        # 3. TRANSFORMATION (The 'Gold' Logic)
        print("🧪 Step 2: Transforming data into Insights...")
        
        # Goal: Calculate avg CPU/RAM per minute or overall
        # We use .agg() for global statistics
        gold_df = silver_df.select(
            F.avg("cpu_usage_pct").alias("avg_cpu_load"),
            F.max("mem_usage_pct").alias("peak_ram_gb"),
            F.min("mem_usage_pct").alias("min_ram_gb"),
            F.count("*").alias("data_points_analyzed")
        )

        # 4. ACTION
        print("\n🏆 GOLD LAYER REPORT:")
        gold_df.show()

        print("\n🕒 Step 3: Hourly Performance Trends")
                
        # We group by the 'hour' partition and calculate stats for that specific block
        hourly_stats = silver_df.groupBy("hour").agg(
            F.avg("cpu_usage_pct").alias("avg_cpu"),
            F.max("mem_usage_pct").alias("peak_mem"),
            F.count("*").alias("samples")
        ).orderBy("hour")
        hourly_stats.show()

        # 5. ADVANCED: High Stress Detection
        # Let's find any moments where CPU was above 80%
        stress_moments = silver_df.filter(F.col("cpu_usage_pct") > 80)
        print(f"⚠️  High Stress Moments (>80% CPU) detected: {stress_moments.count()}")
        if stress_moments.count() > 0:
            stress_moments.show(5)

    except Exception as e:
        print(f"❌ Error during processing: {e}")
        print("Hint: Make sure your Consumer has actually saved .parquet files in the directory.")

    finally:
        print("🔌 Shutting down Spark...")
        spark.stop()
        sys.exit(0)

if __name__ == "__main__":
    main()