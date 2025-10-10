import pandas as pd

class DataCleaner:
    """
    Lớp xử lý và làm sạch dữ liệu tai nạn giao thông
    """
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def run_full_pipeline(self) -> pd.DataFrame:
        """Thực hiện toàn bộ quy trình làm sạch"""
        self.drop_columns()
        self.drop_rows_with_missing()
        self.fill_missing_values()
        self.convert_data_types()
        return self.df

    def drop_columns(self):
        """Xoá các cột không cần thiết"""
        cols = [
            'Source', 'End_Lat', 'End_Lng', 'Description', 'Country',
            'Timezone', 'Airport_Code', 'Weather_Timestamp', 'Wind_Chill(F)',
            'Pressure(in)', 'Wind_Direction', 'Amenity', 'Bump',
            'Give_Way', 'No_Exit', 'Railway'
        ]
        self.df = self.df.drop(columns=cols, errors='ignore')
        print(" Đã xoá cột thừa. Còn lại:", self.df.shape[1], "cột")

    def drop_rows_with_missing(self):
        """Xoá các hàng thiếu dữ liệu quan trọng"""
        important_cols = ["City", "Street", "Sunrise_Sunset",
                          "Astronomical_Twilight", "Civil_Twilight",
                          "Nautical_Twilight", "Zipcode"]
        self.df = self.df.dropna(subset=important_cols)
        print(" Đã xoá hàng thiếu dữ liệu quan trọng. Còn lại:", self.df.shape[0], "hàng")

    def fill_missing_values(self):
        """Điền giá trị thiếu"""
        numeric_cols = ["Temperature(F)", "Humidity(%)", "Visibility(mi)", "Wind_Speed(mph)"]
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna(self.df[col].median())

        if "Weather_Condition" in self.df.columns:
            self.df["Weather_Condition"] = self.df["Weather_Condition"].fillna("Unknown")

        if "Precipitation(in)" in self.df.columns:
            self.df["Precipitation(in)"] = self.df["Precipitation(in)"].fillna(0.0)

        print(" Đã điền giá trị thiếu cho các cột numeric và categorical.")

    def convert_data_types(self):
        """Ép kiểu dữ liệu datetime, bỏ microseconds/nanoseconds"""
        datetime_cols = ["Start_Time", "End_Time"]

        for col in datetime_cols:
            if col in self.df.columns:
                # Bỏ phần microseconds/nanoseconds (sau dấu '.')
                self.df[col] = self.df[col].astype(str).str.split('.').str[0]
                # Ép kiểu datetime, lỗi thành NaT
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')

        print(" Đã ép kiểu datetime, bỏ microseconds/nanoseconds cho Start_Time và End_Time.")
