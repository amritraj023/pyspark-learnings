from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, avg, dense_rank, round, rank, sum, lag, lead, when, first, \
    last, nth_value, broadcast
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

# Exercise: Top selling product per category

#step 1: join order_items with products to get category information

sales_df = (
    order_items_df.alias("oi").join(
        products_df.alias("p"), col("oi.product_id") == col("p.product_id"),
        "inner"
    ).groupBy(
        col("p.category"),
        col("p.product_id"),
        col("p.product_name")
    ).agg(
        sum("oi.quantity").alias("total_quantity_sold"),
        sum(col("oi.quantity") * col("oi.item_price")).alias("total_revenue")
    )
)

# for optimized code

sales_optimized_df = (
    order_items_df.alias("oi").join(
        broadcast(products_df.alias("p")), col("oi.product_id") == col("p.product_id"),
        "inner"
    )
)

# step 2: calculation of rank

windowsSpec = (
    Window.partitionBy("category")
    .orderBy(col("total_revenue").desc())
    .orderBy(col("total_quantity_sold").desc())
)

top_selling_product = (
    sales_df.withColumn(
        "rank", row_number().over(windowsSpec)
    ).filter(col("rank") == 1)
)

print(" top selling product per category")
top_selling_product.show()