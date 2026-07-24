from utils.read_utility import load_csv_file

filename = "events.csv"
events_df = load_csv_file(filename)
events_df.show()