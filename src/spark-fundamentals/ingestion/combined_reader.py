from utils.read_utility import load_csv_file

user_csv = "users.csv"
users_df = load_csv_file(user_csv)
print("users schema: ", users_df.schema)
users_df.show()

orders_csv = "orders.csv"
orders_df = load_csv_file(orders_csv)
print("orders schema: ", orders_df.schema)
orders_df.show()

products_csv = "products.csv"
products_df = load_csv_file(products_csv)
print("products schema: ", products_df.schema)
products_df.show()

events_csv = "events.csv"
events_df = load_csv_file(events_csv)
print("events schema: ", events_df.schema)
events_df.show()

reviews_csv = "reviews.csv"
reviews_df = load_csv_file(reviews_csv)
print("reviews schema: ", reviews_df.schema)
reviews_df.show()

order_items_csv = "order_items.csv"
order_items_df = load_csv_file(order_items_csv)
print("order items schema: ", order_items_df.schema)
order_items_df.show()

print("users count: ", users_df.count())
print("products count: ", products_df.count())
print("orders count: ", orders_df.count())
print("events count: ", events_df.count())
print("reviews count: ", reviews_df.count())
print("order items count: ", order_items_df.count())
