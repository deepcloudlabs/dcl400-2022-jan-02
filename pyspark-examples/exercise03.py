import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, desc, sum
from pyspark.sql.types import StructType, StringType

os.environ["HADOOP_HOME"] = "d:\\stage-bigdata\\opt\\hadoop-3.3.4"
os.environ["HADOOP_COMMON_LIB_NATIVE_DIR"] = "d:\\stage-bigdata\\opt\\hadoop-3.3.4\\lib\\native"

ss = (SparkSession.builder
      .master("local")
      .appName("exercise03-spark streaming")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
      .getOrCreate())

df = ss.readStream.format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("subscribe","binance-trades")\
    .option("startingOffsets", "earliest")\
    .load()

df_schema = StructType() \
    .add("s", StringType()) \
    .add("p", StringType()) \
    .add("q", StringType()) \
    .add("b", StringType()) \
    .add("a", StringType())

df1 = df.select(from_json(col("value").cast("string"), df_schema).alias("trade"))
df1 = df1.withColumn('price', df1['trade.p'].cast("float"))
df1 = df1.withColumn('quantity', df1['trade.q'].cast("float"))
df1 = df1.withColumn('volume', df1['price'] * df1['quantity'])

df1.printSchema()

aggDF = df1.groupby("trade.a").agg(sum("volume").alias("vol"), count("volume")).orderBy(desc("vol"))


df_console_write = aggDF \
    .writeStream \
    .option("numRows", 10) \
    .option("truncate", False) \
    .outputMode("complete") \
    .format("console") \
    .start()

df_console_write.awaitTermination()