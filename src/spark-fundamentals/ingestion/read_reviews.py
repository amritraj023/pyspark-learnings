from utils.read_utility import load_csv_file

filename = "reviews.csv"
reviews_df = load_csv_file(filename)
reviews_df.show()