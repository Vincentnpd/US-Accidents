import os
import sys
import pandas as pd

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(BASE_DIR, "src"))

from src.loader import DataLoader
from src.cleaner import DataCleaner
from src.splitter import DataSplitter

def main():
    file_path = os.path.join(BASE_DIR, "data", "raw", "US_Accidents_March23.csv")
    output_cleaned = os.path.join(BASE_DIR, "data", "processed", "US_Accidents_Cleaned.csv")
    output_split = os.path.join(BASE_DIR, "data", "processed",)

    loader = DataLoader("csv")
    df = loader.load_filtered_csv(path=file_path, start_year=2019, end_year=2023, date_col="Start_Time")

    print("1. Làm sạch dữ liệu")
    cleaner = DataCleaner(df)
    df_clean = cleaner.run_full_pipeline()
    df_clean.to_csv(output_cleaned, index=False)
    print("Đã lưu dữ liệu đã làm sạch tại:", output_cleaned)

    print("2. Tách dữ liệu thành các bảng")
    splitter = DataSplitter(df_clean)
    splitter.export_all(output_split)
    print("XONG NHA AE!")
if __name__ == "__main__":
    main()
