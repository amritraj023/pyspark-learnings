from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
spark.range(10).show()
spark.stop()