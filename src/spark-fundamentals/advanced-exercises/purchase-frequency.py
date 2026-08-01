from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, avg, dense_rank, round, rank, sum, lag, lead, when, first, \
    last, nth_value, broadcast, datediff, min, max
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("lag-lead")
         .master("local[*]").getOrCreate())

DATA_PATH = "../../data/"

order_file = "orders.csv"
products_file = "products.csv"
order_items_file = "order_items.csv"

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + order_file)


# finding previous order

windowSpec = (
    Window.partitionBy("user_id")
    .orderBy("order_date")
)

purchase_gap_df = (
    orders_df.withColumn(
        "previous_order",
        lag("order_date").over(windowSpec)  # lag is used to find the previous value
    ).withColumn(
        "gap_days",
        datediff(col("order_date"), col("previous_order"))
    ).groupBy(
        col("user_id")
    ).agg(
        avg(col("gap_days")).alias("avg_gap_days"),
        min(col("gap_days")).alias("min_gap_days"),
        max(col("gap_days")).alias("max_gap_days")

    )
)

print("purchase order frequency")
purchase_gap_df.show()

