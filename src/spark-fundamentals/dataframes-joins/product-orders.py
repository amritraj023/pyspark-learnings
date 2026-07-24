from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("product-orders") \
    .master("local[*]") \
    .getOrCreate()

DATA_PATH = "../../data/"
product_file = "products.csv"
order_file = "orders.csv"
order_items_file = "order_items.csv"

products_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + product_file)


orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + order_file)


order_items_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA_PATH + order_items_file)


# Find orders with product details

complete_order_details_df = (
    orders_df.alias("o").join(
        order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id"),
        "inner"
    ).join(
        products_df.alias("p"), col("oi.product_id") == col("p.product_id"),
        "inner"
    ).select(
        col("o.order_id"),
        col("p.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("oi.quantity"),
        col("oi.item_price")
    )
)

complete_order_details_df.show()