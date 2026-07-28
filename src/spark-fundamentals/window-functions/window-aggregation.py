from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, avg, dense_rank, round, rank, sum, lag, lead, when, first, \
    last, nth_value
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

windowSpec_reviews = (
    Window.partitionBy("product_id")
    .orderBy("review_date")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

running_average_review_df = (
    reviews_df.withColumn("running_average",
                          round(avg("rating").over(windowSpec_reviews), 2)
                          )
)

print("running average rating df::")
running_average_review_df.show()

# for each category calculate: total sales, Product sales, and percent contribution

product_sales_df = (
    products_df.alias("p").join(order_items_df.alias("o"),
                                col("p.product_id") == col("o.product_id"),
                                "inner"
                                ).groupBy(
        "p.product_id",
        "p.product_name",
        "p.category"
    ).agg(
        sum(col("o.quantity") * col("o.item_price")
            ).alias("product_sales")
    )
)

# window by category
window_category = (
    Window.partitionBy("category")
)

category_sales_df = (
    product_sales_df.withColumn("category_total_sales",
                                sum("product_sales").over(window_category)
                                ).withColumn("percent_sales",
                                             round(col("product_sales") / col("category_total_sales") * 100,2)
                                             )
)

print("category sales df::")
category_sales_df.show()


#first order amount

windowSpec = (
    Window.partitionBy("user_id")
    .orderBy("order_date")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )
)


first_order_df = (
    orders_df.withColumn("first_order_amount",
                         first("total_amount").over(windowSpec)
                         )
)

print("first order for the customer::")
first_order_df.show()


last_order_df = (
    orders_df.withColumn("last_order_amount",
                         last("total_amount").over(windowSpec)
                         )
)

print("last order for the customer::")
last_order_df.show()


# nth order

nth_order_df = (
    orders_df.withColumn("nth_order_amount",
                         nth_value("total_amount", 2).over(windowSpec)
                         )
)

print("2nd order for the customer::")
nth_order_df.show()
