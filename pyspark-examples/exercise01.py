import os

from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = "d:\\stage-bigdata\\opt\\hadoop-3.3.4"
os.environ["HADOOP_COMMON_LIB_NATIVE_DIR"] = "d:\\stage-bigdata\\opt\\hadoop-3.3.4\\lib\\native"

spark = SparkSession.builder.appName("exercise01").getOrCreate()
df = spark.read.json("world.json")
df.show()
df.printSchema()

df.select("name").show()
df.select(df["name"], df["population"] / 1_000_000).show()
df.groupby("continent").count().show()

df.createOrReplaceTempView("countries")

sqlDF = spark.sql("""
    select continent, sum(population) as pop
    from countries
    group by continent
    order by pop desc""")

sqlDF.show()

sqlDF = spark.sql("""
    select count(*)
    from countries
    where population > 100000000""")

sqlDF.show()
