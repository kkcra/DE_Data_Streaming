import sys
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, DoubleType, LongType, IntegerType
from pyspark.sql.window import Window

from time import sleep

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
        print("🛠️  Step 1: Reading Silver Layer (Parquet) with Schema Enforcement...")
        
        # Define the schema based on your successful 'root' printout
        # But we change timestamp to LongType because that's what the error found
        laptop_schema = StructType([
            StructField("timestamp", LongType(), True), # Read as INT64
            StructField("cpu_usage_pct", DoubleType(), True),
            StructField("mem_usage_pct", DoubleType(), True),
            StructField("ctx_switches", LongType(), True),
            StructField("battery_pct", LongType(), True)
            # Year, Month, Day, Hour are inferred from the folder structure automatically
        ])




        # Spark reads all .parquet files in the folder automatically
        # Replace "parquet_data" with your folder name if differents
        # Use .schema() to stop Spark from guessing
        silver_df = spark.read.schema(laptop_schema).parquet("parquet_data")


        # If the timestamp is a Long (Unix Epoch), convert it to a proper Timestamp
        # This makes the Window.orderBy work much better
        silver_df = silver_df.withColumn("timestamp", F.from_unixtime(F.col("timestamp") / 1000).cast("timestamp"))
        # (Note: If your INT64 is in milliseconds, divide by 1000. If it's in seconds, remove the / 1000.)






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
        
        #####################################

        # 6. Window Functions: Moving Average

        # DEFINE THE WINDOW
        # We want to order by time. We don't partition here because 
        # we want a global trend across all hardware data.
        # rowsBetween(-5, 0) means: "Take the current row and the 5 previous rows"

        moving_avg_window = Window.orderBy("timestamp").rowsBetween(-5, 0)

        # APPLY WINDOW FUNCTIONS
        print("")
        print("🧪 Calculating Rolling Metrics...")

        gold_trends_df = ( silver_df
                            .withColumn("rolling_avg_cpu", F.avg("cpu_usage_pct").over(moving_avg_window))
                            .withColumn("cpu_diff", F.col("cpu_usage_pct") - F.col("rolling_avg_cpu"))
        )

        # Why No Commas? (Method Chaining)
        # This is the biggest hurdle for SQL users moving to PySpark. The reason there are no commas is that PySpark uses Method Chaining (also known as a Fluent Interface).

        gold_trends_df.show()

        gold_trends_df.select("timestamp", "cpu_usage_pct", "rolling_avg_cpu").show(10)



        # SHOW THE RESULTS
        # We select only relevant columns to see the "smoothing" effect
        print("\n📈 WINDOW FUNCTION OUTPUT (Individual Rows + Rolling Stats):")
        gold_trends_df.select(
            "timestamp", 
            "cpu_usage_pct", 
            F.round("rolling_avg_cpu", 2).alias("moving_avg"),
            F.round("cpu_diff", 2).alias("spike_intensity")
        ).show(20, truncate=False)



        #####################################

    except Exception as e:
        print(f"❌ Error during processing: {e}")
        print("Hint: Make sure your Consumer has actually saved .parquet files in the directory.")

    finally:
        print("")
        print("🔌 Shutting down Spark...")
        print("")
        spark.stop()
        sys.exit(0)

        sleep(3)

if __name__ == "__main__":
    main()