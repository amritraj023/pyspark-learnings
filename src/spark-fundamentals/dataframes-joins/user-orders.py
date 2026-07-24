from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, sum, round

spark = (SparkSession.builder.appName("user-orders")
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

#customer orders: inner join

customer_orders_df = (
    users_df.alias("u")
    .join(
        orders_df.alias("o"), col("u.user_id") == col("o.user_id"),
        "inner"
    ).select(
        col("u.user_id"),
        col("u.name"),
        col("o.order_id"),
        col("o.order_status"),
        col("o.total_amount")
    )
)

customer_orders_df.show()

# customers without any orders

customer_without_orders_df = (

    users_df.alias("u")
    .join(
        orders_df.alias("o"), col("u.user_id") == col("o.user_id"),
        "left"
    ).filter(col("o.order_id").isNull())
    .select(
        col("u.user_id"),
        col("u.name"),
        col("u.city")
    )
)
customer_without_orders_df.show()

# using left-anti join

customers_without_orders_anti_df = (
    users_df.alias("u")
    .join(
        orders_df.alias("o"), col("u.user_id") == col("o.user_id"),
        "left_anti"
    ).select(
        col("u.user_id"),
        col("u.name"),
        col("u.city")
    )
)

customers_without_orders_anti_df.show()

# top 10 customers by revenue

top_users_df = (
    users_df.alias("u")
    .join(
        orders_df.alias("o"), col("u.user_id") == col("o.user_id"),
        "inner"
    ).groupBy(
        col("u.user_id"),
        col("u.name"),
    ).agg(
        sum("o.total_amount").alias("total_revenue"),
        count("o.order_id").alias("total_orders"),
        round(avg("o.total_amount"),2).alias("average_order_value")
    ).orderBy(desc("total_revenue"))
)

print("top user purchases::")
top_users_df.show(10)
