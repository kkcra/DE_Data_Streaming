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