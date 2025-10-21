import os
import sys

# Thêm thư mục gốc của project vào sys.path
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(BASE_DIR, "src"))

from src.loader import DataLoader
from src.cleaner import DataCleaner
from src.splitter import DataSplitter


def main():
    # 1. Nạp dữ liệu gốc
    file_path = os.path.join(BASE_DIR, "data", "raw", "US_Accidents_March23.csv")
    loader = DataLoader("csv")
    df = loader.load_from_csv(file_path)

    # 2. Làm sạch dữ liệu
    cleaner = DataCleaner(df)
    df_clean = cleaner.run_full_pipeline()

    # Kiểm tra kiểu dữ liệu của 2 cột thời gian
    print("\nKiểu dữ liệu sau khi ép:")
    print(df_clean[["Start_Time", "End_Time"]].dtypes)

    # 3. Lưu dữ liệu sau xử lý
    output_clean_path = os.path.join(BASE_DIR, "data", "processed", "clean_accidents.csv")
    os.makedirs(os.path.dirname(output_clean_path), exist_ok=True)
    df_clean.to_csv(output_clean_path, index=False)

    print(f"\nDữ liệu sạch đã được lưu tại: {output_clean_path}")
    print(f"Kích thước cuối cùng: {df_clean.shape[0]} hàng, {df_clean.shape[1]} cột")

    # 4. Tách dữ liệu thành nhiều bảng
    processed_dir = os.path.join(BASE_DIR, "data", "processed")
    splitter = DataSplitter(df_clean)
    splitter.split_and_save(processed_dir)

    print("\nHoàn tất toàn bộ pipeline xử lý và tách bảng.")


if __name__ == "__main__":
    main()
