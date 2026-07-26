from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, avg, dense_rank, round, rank, sum, lag, lead, when
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("lag-lead")
         .master("local[*]").getOrCreate())

DATA_PATH = "../../data/"

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

windowSpec_order = (
    Window.partitionBy("user_id")
    .orderBy("order_date")
)

order_comparison_df = (
    orders_df.withColumn("previous_amount",
                         lag("total_amount").over(windowSpec_order)
                         ).withColumn("difference_amount",
                                      col("total_amount") - col("previous_amount")
                                      )
)

print("order comparison data with lag")
order_comparison_df.show()

# Calculate percentage increase/decrease from previous order

growth_df = (
    orders_df.withColumn("previous_amount",
                         lag("total_amount").over(windowSpec_order)
                         ).withColumn("growth_percentage",
                                      when(
                                          col("previous_amount").isNull(),
                                          None
                                      ).when(
                                          col("previous_amount") == 0,
                                          None
                                      ).otherwise(
                                          round(
                                              (
                                                      (col("total_amount") - col("previous_amount")) / col(
                                                  "previous_amount")
                                              ) * 100, 2
                                          )
                                      )
                                      )
)

print("growth data with lag")
growth_df.show()

# next order date
next_order_df = (
    orders_df.withColumn("next_order_date",
                         lead("order_date").over(windowSpec_order)
                         )
)

print("next order date with lead")
next_order_df.show()

# Compare current review rating with previous review

windowSpec_review = (
    Window.partitionBy("product_id")
    .orderBy("review_date")
)

review_df = (
    reviews_df.withColumn("previous_rating",
                          lag("rating").over(windowSpec_review)
                          ).withColumn("status",
                                       when(col("previous_rating").isNull(), "first review")
                                       .when(col("rating") > col("previous_rating"), "improved")
                                       .when(col("rating") < col("previous_rating"), "declined")
                                       .otherwise("No changes")
                                       )
)
print("review data with lag")
review_df.show()

# Find customers whose latest order amount is greater than their previous order amount

history_window = (
    Window.partitionBy("user_id")
    .orderBy("order_date")
)

latest_window = (
    Window.partitionBy("user_id")
    .orderBy(desc(col("order_date")))
)

customer_growth_df = (

    orders_df.withColumn("previous_amount",
                         lag("total_amount").over(history_window)
                         ).withColumn("order_rank",
                                      row_number().over(latest_window)
                                      ).filter(
        (col("order_rank") == 1) &
        (col("total_amount") > col("previous_amount"))
    ).drop("order_rank")
)

print("customer growth data with lag")
customer_growth_df.show()

# Find customers whose next order is smaller than the current order

declining_orders_df = (
    orders_df.withColumn("next_amount",
                         lead("total_amount").over(history_window)
                         ).filter(
        (col("next_amount").isNotNull()) &
        (col("total_amount") > col("next_amount"))
    )
)

print("declining orders data with lead")
declining_orders_df.show()
