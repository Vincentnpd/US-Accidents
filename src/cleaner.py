import pandas as pd

class DataCleaner:
    """Lớp xử lý và làm sạch dữ liệu tai nạn giao thông"""
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def run_full_pipeline(self) -> pd.DataFrame:
        """Thực hiện toàn bộ quy trình làm sạch"""
        self.drop_columns()
        self.fill_all_nulls_with_zero()
        self.convert_booleans_to_int()
        self.convert_data_types()
        return self.df

    def drop_columns(self):
        """Xoá các cột không cần thiết"""
        cols = [
            'End_Lat', 'End_Lng', 'Start_Lat', 'Start_Lng',
            'Sunrise_Sunset', 'Civil_Twilight',
            'Nautical_Twilight', 'Astronomical_Twilight'
        ]
        self.df = self.df.drop(columns=cols, errors='ignore')
        print("Đã xoá cột thừa. Còn lại:", self.df.shape[1], "cột")

    def fill_all_nulls_with_zero(self):
        """Chuyển toàn bộ giá trị null thành 0"""
        self.df = self.df.fillna(0)
        print("Đã thay toàn bộ giá trị null bằng 0.")

    def convert_booleans_to_int(self):
        """Chuyển giá trị True/False thành 1/0 trong các cột liên quan"""
        bool_cols = [
            "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
            "Railway", "Roundabout", "Station", "Stop",
            "Traffic_Calming", "Traffic_Signal", "Turning_Loop"
        ]
        for col in bool_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(int)
        print("Đã đổi True/False thành 1/0 cho các cột boolean.")

    def convert_data_types(self):
        """Ép kiểu dữ liệu datetime, bỏ microseconds/nanoseconds"""
        datetime_cols = ["Start_Time", "End_Time"]
        for col in datetime_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str).str.split('.').str[0]
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        print("Đã ép kiểu datetime cho Start_Time và End_Time.")
