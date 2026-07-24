from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("user-orders-window")
         .master("local[*]")
         .getOrCreate())

DATA_PATH = "../../data/"
user_file = "users.csv"
order_file = "orders.csv"

users_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + user_file)

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + order_file)

windowSpec = (
    Window.partitionBy("user_id")
    .orderBy("order_date")
)

result = (
    orders_df.withColumn("row_num",
                         row_number().over(windowSpec)
                         )
)
result.show()

# show the result for each user for latest order

windowSpec_latest = (
    Window.partitionBy("user_id")
    .orderBy(desc(col("order_date")))
)

latest_order_df = (
    orders_df.withColumn("order_rank",
                         row_number().over(windowSpec_latest)
                         ).filter(col("order_rank") == 1)
)
latest_order_df.show()

#highest order per customer

windowSpec_highest = (
    Window.partitionBy("user_id")
    .orderBy(desc(col("total_amount")))
)

highest_order_df = (
    orders_df.withColumn("order_rank_highest",
                         row_number().over(windowSpec_highest)
                         ).filter(col("order_rank_highest") == 1)
)
highest_order_df.show()

# top 3 orders per customer

windowSpec_top3 = (
    Window.partitionBy("user_id")
    .orderBy(desc(col("total_amount")))
)

top3_orders_df = (
    orders_df.withColumn("order_rank_top3",
                         row_number().over(windowSpec_top3)
                         ).filter(col("order_rank_top3") <= 3)
)
top3_orders_df.show()


#Deduplicate records: removal of duplicate records
windowSpec_deduplicate = (
    Window.partitionBy("user_id")
    .orderBy(desc(col("order_date")))
)

deduplicate_order_df = (
    orders_df.withColumn("order_rank",
                         row_number().over(windowSpec_deduplicate)
                         ).filter(col("order_rank") == 1).drop("order_rank")
)
deduplicate_order_df.show()