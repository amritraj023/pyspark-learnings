from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, avg, dense_rank, round, rank, sum, lag, lead, when
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("lag-lead")
         .master("local[*]").getOrCreate())



DATA_PATH = "../../data/"
user_file = "users.csv"
order_file = "orders.csv"
reviews_file = "reviews.csv"

users_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + user_file)

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + order_file)

reviews_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + reviews_file)


# Calculate cumulative spending for every customer.

windowSpec_spend = (
    Window.partitionBy("user_id")
    .orderBy("order_date")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

running_total_df = (
    orders_df.withColumn("running_total",
                         sum("total_amount").over(windowSpec_spend)
                         )
)
print("running total df::")
running_total_df.show()

# running average
running_average_df = (
    orders_df.withColumn("running_average",
                         avg("total_amount").over(windowSpec_spend)
                         )
)
print("running average df::")
running_average_df.show()
