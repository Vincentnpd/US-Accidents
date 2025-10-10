<<<<<<< HEAD
from src.data_loader import DataLoader
from src.data_cleaner import DataCleaner

def main():
    # Load dữ liệu
    loader = DataLoader()
    file_path = r"D:/DATN/US - Accidents/data/raw/US_Accidents_March23.csv"
    df = loader.load_from_csv(file_path)
    print(" Dữ liệu gốc:", df.shape)

    # Xoá cột thừa
    columns_to_drop = [
        'Source', 'End_Lat', 'End_Lng', 'Description', 'Country', 
        'Timezone', 'Airport_Code', 'Weather_Timestamp', 'Wind_Chill(F)', 
        'Pressure(in)', 'Wind_Direction', 'Amenity', 'Bump', 
        'Give_Way', 'No_Exit', 'Railway'
    ]
    cleaner = DataCleaner(df)
    df_cleaned = cleaner.drop_columns(columns_to_drop).get_data()
    print("Sau khi xoá cột:", df_cleaned.shape)

    # Xoá hàng thiếu dữ liệu quan trọng (<1%)
    important_cols = ["City", "Street", "Sunrise_Sunset", 
                      "Astronomical_Twilight", "Civil_Twilight", 
                      "Nautical_Twilight", "Zipcode"]
    df_cleaned = DataCleaner(df_cleaned).drop_rows_with_missing(important_cols).get_data()

    # Điền median cho các cột số (2–7% missing)
    numeric_cols = ["Temperature(F)", "Humidity(%)", "Visibility(mi)", "Wind_Speed(mph)"]
    df_cleaned = DataCleaner(df_cleaned).fill_numeric_with_median(numeric_cols).get_data()
    

    # Điền 'Unknown' cho categorical
    df_cleaned = DataCleaner(df_cleaned).fill_categorical_with_unknown(["Weather_Condition"]).get_data()
    

    # Điền 0.0 cho Precipitation(in)
    df_cleaned = DataCleaner(df_cleaned).fill_precipitation().get_data()
    # Kiểm tra số giá trị null trong từng cột
    print("Số giá trị null trong từng cột sau khi clean:")
    print(df_cleaned.isnull().sum())

=======
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
>>>>>>> d4339f4 (cleaned'new')


if __name__ == "__main__":
    main()
