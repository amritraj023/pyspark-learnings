from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Read Orders")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

DATA_PATH = "../../data/"


def load_csv_file(filename):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_PATH + filename)
    )
