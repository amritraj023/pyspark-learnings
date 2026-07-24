from pyspark.sql.functions import col, round
import pyspark.sql.functions as F

from utils.read_utility import load_csv_file

filename = "products.csv"
products_df = load_csv_file(filename)
#products_df.show()

# Show distinct categories

distinct_category = (products_df.select("category")
                     .distinct().orderBy("category").show())


# exercise

products_df_select = products_df.select(
    "product_name",
    "category",
    "price"
).show()

products_df_renamed = products_df.withColumnRenamed(
    "product_name", "Product"
).withColumnRenamed(
    "price", "Selling Price"
).show()


products_df_add_column = products_df.withColumn(
    "GST", col("price") * 0.18
).withColumn(
    "GST", round(col("GST"), 2)
).withColumn(
    "Final Price", col("price") + col("GST")
).withColumn(
    "Final Price", round(col("Final Price"), 2)
).show()

#altername method

products_df_add_column_2 = (
    products_df
.withColumn("GST", F.round(F.col("price") * 0.18 ,2))
.withColumn("Final Price",F.round((F.col("price") + F.col("GST")),2)
)).show()

