
import os
import sys

# Thêm thư mục gốc của project vào sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.loader import DataLoader
from src.cleaner import DataCleaner


def main():
    # Đảm bảo có thể import module src từ bất kỳ vị trí nào
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    #  Nạp dữ liệu
    file_path = r"D:/DATN/US - Accidents/data/raw/US_Accidents_March23.csv"
    loader = DataLoader("csv")
    df = loader.load_from_csv(file_path)

    #  Làm sạch dữ liệu
    cleaner = DataCleaner(df)
    df_clean = cleaner.run_full_pipeline()

    # Kiểm tra kiểu dữ liệu của 2 cột thời gian
    print(df_clean[["Start_Time", "End_Time"]].dtypes)

    #  Lưu dữ liệu sau xử lý
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "processed", "clean_accidents.csv"))
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_clean.to_csv(output_path, index=False)

    print("\n Pipeline hoàn tất!")
    print(f" Dữ liệu đã được lưu tại: {output_path}")
    print(f" Kích thước cuối cùng: {df_clean.shape[0]} hàng, {df_clean.shape[1]} cột")


if __name__ == "__main__":
    main()
