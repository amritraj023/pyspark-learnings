from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, avg, dense_rank, round, rank, sum, lag, lead, when, first, \
    last, nth_value, count
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("lag-lead")
         .master("local[*]").getOrCreate())

DATA_PATH = "../../data/"
user_file = "users.csv"
order_file = "orders.csv"
reviews_file = "reviews.csv"
products_file = "products.csv"
order_items_file = "order_items.csv"

reviews_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + reviews_file)

order_items_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(DATA_PATH + order_items_file)
)

products_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(DATA_PATH + products_file)
)

# Most reviewed product

# step 1: review summary

review_summary = (
    reviews_df.groupBy("product_id")
    .agg(
        count("review_id").alias("total_reviews"),
        avg("rating").alias("avg_rating")
    )
)

revenue_summary = (
    order_items_df.groupBy("product_id")
    .agg(
        sum(col("quantity") * col("item_price")).alias("total_revenue")
    )
)

summary = (
    products_df.alias("p").join(
        review_summary.alias("r"), col("p.product_id") == col("r.product_id"),
        "inner"
    ).join(
        revenue_summary.alias("rs"), col("p.product_id") == col("rs.product_id"),
        "inner"
    )
)

# ranking the reviewed products based on total reviews and total revenue

windowSpec = (
    Window.partitionBy("category").orderBy(
        col("total_reviews").desc(),
        col("avg_rating").desc(),
        col("total_revenue").desc()
    )
)

winner_df = (
    summary.withColumn(
        "rank", row_number().over(windowSpec)
    ).filter(col("rank") == 1)
)

print("most reviewed product overall::")
winner_df.show()
