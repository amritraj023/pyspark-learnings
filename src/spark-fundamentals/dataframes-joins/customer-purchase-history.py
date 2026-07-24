from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc, count, round

spark = (SparkSession.builder.appName("customer-purchase-history")
         .master("local[*]").getOrCreate())

DATA_PATH = "../../data/"

user_file = "users.csv"
order_file = "orders.csv"
product_file = "products.csv"
order_items_file = "order_items.csv"
reviews_file = "reviews.csv"

users_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + user_file)

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + order_file)

products_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + product_file)

order_items_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + order_items_file)


reviews_df = spark.read.format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(DATA_PATH + reviews_file)

user_purchase_history_df = (
    users_df.alias("u")
    .join(orders_df.alias("o"),
          col("u.user_id") == col("o.user_id"),
          "inner"
    ).join(
        order_items_df.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
        "inner"
    ).join(
        products_df.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "inner"
    ).select(
        col("u.name"),
        col("p.product_name"),
        col("oi.quantity"),
        col("o.order_date"),
        col("o.total_amount")
    )

)

user_purchase_history_df.show()

# products never purchased

products_never_purchased_df = (
    products_df.alias("p").join(
        order_items_df.alias("oi"), col("p.product_id") == col("oi.product_id"),
        "left_anti"
    ).select(
        col("p.product_id"),
        col("p.product_name")
    )
)
print("products_never_purchased_df:")
products_never_purchased_df.show()

# average product rating

average_products_df = (
    products_df.alias("p").join(
        reviews_df.alias("r"), col("p.product_id") == col("r.product_id"),
        "inner"
    ).groupBy(
        col("p.product_id"),
        col("p.product_name")
    ).agg(
        avg("r.rating").alias("average_rating")
    ).orderBy(desc("average_rating"))
)

average_products_df.show()

# number of reviews written by each user

user_reviews_df = (
    users_df.alias("u").join(
        reviews_df.alias("r"), col("u.user_id") == col("r.user_id"),
        "inner"
    ).groupBy(
        col("u.user_id"),
        col("u.name")
    ).agg(
        count("r.review_id").alias("num_reviews")
    ).orderBy(desc("num_reviews"))
)

print("reviews by each user")
user_reviews_df.show()

# top-rated products having at least 10 reviews.

products_reviews_df = (
    products_df.alias("p").join(
        reviews_df.alias("r"), col("p.product_id") == col("r.product_id"),
        "inner"
    ).groupBy(
        col("p.product_id"),
        col("p.product_name")
    ).agg(
        round(avg("r.rating"),2).alias("avg_rating"),
        count("r.review_id").alias("reviews_count"),
    ).filter(col("reviews_count") >= 10)
    .orderBy(desc("avg_rating"))
)
print("products_reviews_df:")
products_reviews_df.show()