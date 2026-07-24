from pyspark.sql.functions import *

from utils.read_utility import load_csv_file

filename = "orders.csv"
orders_df = load_csv_file(filename)
#orders_df.show()

# provides basic statistics for each column
orders_df.describe().show()

# order status

order_status = orders_df.select("order_status").distinct() \
    .orderBy("order_status").show()

orders_df_updated = orders_df.withColumn(
    "order_status", when(col("order_status") == "cancelled", "canceled")
    .otherwise(col("order_status"))
)
orders_df_updated.show()

orders_df_shipped = orders_df.where(col("order_status") == "shipped")
orders_df_shipped.show()

orders_df_count = orders_df.where(
    (col("order_status") == "shipped") &
    (col("total_amount") > 500)
)
orders_df_count.show()

# using filter

orders_df_filter = orders_df.filter(
    (col("order_status") == "completed") &
    (col("total_amount") > 100)
)
orders_df_filter.show()

# group by
orders_grouped = orders_df.groupBy("order_status").count().show()

# customers spending more than 1000
orders_df_high_spenders = orders_df.filter(
    col("total_amount") > 1000
    ).groupBy("user_id").count().show()

# average shipped order value

orders_df_average_shipped = orders_df.filter(
    (col("order_status") == "shipped")).groupBy("order_status") \
    .avg("total_amount").show()


# modify existing column
orders_df_modify = orders_df.withColumn("total_amount", col("total_amount") * 1.1)
orders_df_modify.show()

# rounding off
orders_df_roundup = orders_df.withColumn("total_amount", round(col("total_amount"), 2))
orders_df_roundup.show()


#exercises: Display all shipped orders where amount > ₹1500 sorted by amount descending


orders_df_filter_1500 = orders_df.filter(
    (col("order_status") == "shipped") &
    (col("total_amount") > 1500)
).orderBy(desc("total_amount")).show()

#exercise 2: Create Order Size column

result_df = orders_df.withColumn(
    "order_size",
    when(col("total_amount") > 3000, "Large")
    .when(col("total_amount") > 1000, "Medium")
    .otherwise("Small")
).show()

# Average Order Amount by Status

av_orders_df = (
    orders_df.groupBy("order_status")
    .agg(avg("total_amount").alias("avg_amount")).show()
)

#total revenue per status
total_orders_df = (
    orders_df.groupBy("order_status")
    .agg(sum("total_amount").alias("total_revenue")).show()
)