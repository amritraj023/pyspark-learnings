from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, avg, dense_rank, round, rank, sum
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("customer-purchase-history")
         .master("local[*]").getOrCreate())

DATA_PATH = "../../data/"

reviews_file = "reviews.csv"
products_file = "products.csv"
order_items_file = "order_items.csv"

reviews_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + reviews_file)

products_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(DATA_PATH + products_file)
)

order_items_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(DATA_PATH + order_items_file)
)


window_spec = (
    Window.partitionBy("product_id")
    .orderBy("review_date")
)

# review ranking

reviews_ranked_df = reviews_df.withColumn("review_rank",
                                          row_number().over(window_spec)
                                          )
reviews_ranked_df.show()

# latest review per product
latest_reviews_df = reviews_ranked_df.filter(col("review_rank") == 1)
latest_reviews_df.show()

# remove duplicate reviews and keep only the latest review per user id, product id

window_spec_product = (
    Window.partitionBy("user_id", "product_id")
    .orderBy(desc("review_date"))
)

latest_reviews_df = latest_reviews_df.withColumn("review_rank",
                                                 row_number().over(window_spec_product)
                                                 ).filter(col("review_rank") == 1).drop("review_rank")
print("latest reviews after removing duplicates:")
latest_reviews_df.show()

# Assign a dense_rank() to products based on average rating within each category

# calculate average rating per product and category
products_rating_df = (
    products_df.alias("p").join(
        reviews_df.alias("r"), col("p.product_id") == col("r.product_id"),
        "inner"
    ).groupBy(
        "p.product_id",
        "p.product_name",
        "p.category"
    ).agg(
        avg(round(col("r.rating"), 2)).alias("avg_rating"),
    )
)

# creating window specs
windowSpec_product = (
    Window.partitionBy("category")
    .orderBy(desc(col("avg_rating")))
)

products_rating_df = (
    products_rating_df.withColumn("category_rank",
                                  dense_rank().over(windowSpec_product)
                                  )
)

print("products ranked by category using dense ranking:")
products_rating_df.show()

# top 2 products per category
top2_rated_products_df = (
    products_rating_df.withColumn("category_rank",
                                  dense_rank().over(windowSpec_product)
                                  )
).filter(col("category_rank") <= 2)

print("top 2 products ranked by category using dense ranking:")
top2_rated_products_df.show()

# rank reviews for each product by review date

windowSpec_product = (
    Window.partitionBy("product_id")
    .orderBy(desc("review_date"))
)

reviews_ranking_df = (
    reviews_df.withColumn("review_rank",
                          rank().over(windowSpec_product))
)

print("rank reviews for each product by review date:")
reviews_ranking_df.show()


# top selling product in each category

product_sales_df = (
    products_df.alias("p").join(
        order_items_df.alias("i"),col("p.product_id") == col("i.product_id"),
        "inner"
    ).groupBy(
        "p.product_id",
        "p.product_name",
        "p.category"
    ).agg(
        sum(col("i.quantity")).alias("total_quantity")
    )
)

windowSpec_top_selling = (
    Window.partitionBy("category")
    .orderBy(desc("total_quantity"))
)

top_selling_product_df = (
    product_sales_df.withColumn("rank",
                                row_number().over(windowSpec_top_selling)
                                ).filter(col("rank") == 1).drop("rank")

)

print("top selling products by category using row_number():")
top_selling_product_df.show()