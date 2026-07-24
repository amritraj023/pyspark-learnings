from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

spark = (
    SparkSession.builder.appName("User details with explicit schema")
    .master("local[*]")
    .getOrCreate())

user_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True),

])

DATA_PATH = "../../data/"
user_file = "users.csv"

users_df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(user_schema)
    .load(DATA_PATH + user_file)
)

users_df.show()


