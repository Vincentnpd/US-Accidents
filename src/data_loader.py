import pandas as pd

class DataLoader:
    def __init__(self, source="csv"):
        self.source = source

    def load_from_csv(self, file_path):
        return pd.read_csv(file_path)
