from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("customer-purchase-history")
         .master("local[*]").getOrCreate())

DATA_PATH = "../../data/"

reviews_file = "reviews.csv"

reviews_df = spark.read.format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(DATA_PATH + reviews_file)


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

window_spec_product =(
    Window.partitionBy("user_id", "product_id")
    .orderBy(desc("review_date"))
)

latest_reviews_df = latest_reviews_df.withColumn("review_rank",
                                                 row_number().over(window_spec_product)
                                                 ).filter(col("review_rank") == 1).drop("review_rank")

latest_reviews_df.show()