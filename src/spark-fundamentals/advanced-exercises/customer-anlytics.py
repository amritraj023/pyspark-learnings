from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, avg, dense_rank, round, rank, sum, lag, lead, when, first, \
    count, last, nth_value, min, max, datediff, current_date, asc
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("lag-lead")
         .master("local[*]").getOrCreate())

DATA_PATH = "../../data/"
user_file = "users.csv"
order_file = "orders.csv"
reviews_file = "reviews.csv"
products_file = "products.csv"
order_items_file = "order_items.csv"

users_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + user_file)

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + order_file)

# Exercise 1 — Customer Lifetime Analytics

# step 1: aggregate customer orders

customer_orders_df = (
    orders_df.groupBy("user_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("total_spend"),
        avg("total_amount").alias("avg_order_value"),
        min("order_date").alias("first_order_date"),
        max("order_date").alias("latest_order_date"),
    )
)

# step 2: customer lifetime days
customer_orders_df = (customer_orders_df.withColumn(
    "customer_lifetime_days",
    datediff(col("latest_order_date"), col("first_order_date")))
)

# step 3 : days since last order
customer_orders_df = (
    customer_orders_df.withColumn("days_since_last_order",
                                  datediff(current_date(), "latest_order_date"))
)

# step 4: rank customers:

window_spec = Window.orderBy(
    desc("total_spend")
)

customer_orders_df = (
    customer_orders_df.withColumn(
        "customer_rank", rank().over(window_spec)
    )
)

# step 5 : join with users to get the complete analytics
customer_lifetime_orders = (
    users_df.alias("u").join(
        customer_orders_df.alias("co"),
        col("u.user_id") == col("co.user_id"),
        "left"
    )
)

print("complete customer analytics::")

customer_lifetime_orders = customer_lifetime_orders.select(
    col("u.user_id"),
    col("u.name").alias("customer_name"),
    col("total_orders"),
    col("total_spend"),
    col("avg_order_value"),
    col("first_order_date").alias("first_order"),
    col("latest_order_date").alias("latest_order"),
    col("days_since_last_order").alias("days_since_last"),
    col("customer_lifetime_days").alias("lifetime_days"),
    col("customer_rank").alias("rank"),
).orderBy(
    desc("total_spend"))

customer_lifetime_orders.show()
