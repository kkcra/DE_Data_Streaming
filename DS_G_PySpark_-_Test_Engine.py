import sys 
import findspark
import os

# Ensuring environment variables are explicitly set for this session
os.environ['JAVA_HOME'] = r'C:\PROGRA~1\Eclipse Adoptium\jdk-17.0.18.8-hotspot'
os.environ['SPARK_HOME'] = r'C:\spark'

findspark.init()

from pyspark.sql import SparkSession

# Wrap the entire process in a try block
try:
    print("context: Waking the Lion and opening the Py4J Bridge...")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("DE_Project_Gold_Layer") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()

    # This won't stop the Windows PID messages (those are OS-level), but it will stop the "WARNING: incubator modules" and other Java noise.
    spark.sparkContext.setLogLevel("OFF")

    # Create a small dataset to test
    data = [("CPU_High", 95.5), ("Memory_OK", 40.2), ("Battery_Low", 15.0)]
    columns = ["Status", "Value_Pct"]

    df = spark.createDataFrame(data, columns)

    print("\n🚀 Spark is Alive! Here is your Dataframe:")
    df.show()

except Exception as e:
    print(f"❌ An error occurred during Spark execution: {e}")

finally:
    # This block runs NO MATTER WHAT
    if 'spark' in locals():
        print("\ncontext: Signaling Spark to shut down...")
        spark.stop()
        print("✅ Spark session stopped.")
        
    # This is the "Emergency Brake" to get your PowerShell prompt back
    print("context: Forcing Python process exit...")
    sys.exit(0)

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