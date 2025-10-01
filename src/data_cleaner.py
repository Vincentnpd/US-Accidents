import pandas as pd

class DataCleaner:
    def __init__(self, df):
        self.df = df.copy()  # giữ bản sao dữ liệu gốc

    # Xoá các cột không cần thiết
    def drop_columns(self, columns_to_drop):
        self.df = self.df.drop(columns=columns_to_drop, errors="ignore")
        return self  # cho phép chaining

    # Xoá các hàng thiếu dữ liệu trong các cột quan trọng
    def drop_rows_with_missing(self, subset_cols):
        self.df = self.df.dropna(subset=subset_cols)
        return self

    # Điền median cho các cột số
    def fill_numeric_with_median(self, numeric_cols):
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna(self.df[col].median())
        return self

    # Điền giá trị "Unknown" cho các cột categorical
    def fill_categorical_with_unknown(self, cat_cols):
        for col in cat_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna("Unknown")
        return self

    # Điền 0.0 cho Precipitation(in)
    def fill_precipitation(self):
        if "Precipitation(in)" in self.df.columns:
            self.df["Precipitation(in)"] = self.df["Precipitation(in)"].fillna(0.0)
        return self

    

    # Lấy dữ liệu cuối cùng
    def get_data(self):
        return self.df
