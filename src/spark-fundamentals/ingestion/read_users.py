from pyspark.sql.functions import col, split

from utils.read_utility import load_csv_file

filename = "users.csv"
users_df = load_csv_file(filename)
#users_df.show()

# showing distinct cities
distinct_cities = (
    users_df.select("city")
    .distinct()
    .orderBy("city").show(truncate=False)
)

# selecting columns
users_df_select = users_df.select(
    "user_id",
    "name",
    "email"
).show()

# aliasing columns
users_df_alias = users_df.select(col("name").alias("fullname")).show()

# renaming columns
users_df_rename = users_df.withColumnRenamed("name", "fullname")
users_df_rename.printSchema()

# adding new column
users_df_add_column = users_df.withColumn("first_name", split(col("name"), " ").getItem(0)) \
                        .withColumn("last_name", split(col("name"), " ").getItem(1))

users_df_add_column.printSchema()
users_df_add_column.show()

# finding duplicate emails
duplicate_emails = (
    users_df.groupBy("email")
    .count().filter(col("count") > 1)
).show()

# Customers from specific cities sorted by name

users_df_cities = (
    users_df.filter(
        col("city").isin(
            "North Collin",
            "Jamesmouth",
            "Chicago"
        )
    ).orderBy("name")
).show()

