from utils.read_utility import load_csv_file

filename = "order_items.csv"
order_items_df = load_csv_file(filename)
order_items_df.show()