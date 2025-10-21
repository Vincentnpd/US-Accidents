import pandas as pd
import os

class DataSplitter:
    """
    Lớp chịu trách nhiệm tách dữ liệu sạch thành các bảng nhỏ hơn.
    """
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def split_and_save(self, output_dir: str):
        """Tách dữ liệu và lưu thành nhiều bảng CSV"""

        os.makedirs(output_dir, exist_ok=True)

        # 1. Bảng vị trí địa lý
        vi_tri_cols = ['Street', 'City', 'County', 'State', 'Zipcode']
        vi_tri = self.df[vi_tri_cols].drop_duplicates().reset_index(drop=True)
        vi_tri['Location_ID'] = range(1, len(vi_tri) + 1)

        # 2. Bảng điều kiện thời tiết
        weather_cols = ['Temperature(F)', 'Humidity(%)', 'Visibility(mi)',
                        'Wind_Speed(mph)', 'Precipitation(in)', 'Weather_Condition']
        thoi_tiet = self.df[weather_cols].drop_duplicates().reset_index(drop=True)
        thoi_tiet['Weather_ID'] = range(1, len(thoi_tiet) + 1)

        # 3. Bảng hạ tầng giao thông
        infra_cols = ['Crossing', 'Junction', 'Roundabout', 'Station', 'Stop',
                      'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
        ha_tang = self.df[infra_cols].drop_duplicates().reset_index(drop=True)
        ha_tang['Infra_ID'] = range(1, len(ha_tang) + 1)

        # 4. Bảng điều kiện ánh sáng
        light_cols = ['Sunrise_Sunset', 'Civil_Twilight',
                      'Nautical_Twilight', 'Astronomical_Twilight']
        anh_sang = self.df[light_cols].drop_duplicates().reset_index(drop=True)
        anh_sang['Light_ID'] = range(1, len(anh_sang) + 1)

        # 5. Bảng thời gian
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'], errors='coerce')
        unique_dates = self.df['Start_Time'].dt.date.dropna().unique()
        thoi_gian = pd.DataFrame({
            'Time_ID': range(1, len(unique_dates) + 1),
            'Date': unique_dates
        })
        thoi_gian['Year'] = pd.to_datetime(thoi_gian['Date']).dt.year
        thoi_gian['Month'] = pd.to_datetime(thoi_gian['Date']).dt.month
        thoi_gian['Day'] = pd.to_datetime(thoi_gian['Date']).dt.day

        # 6. Bảng tai nạn (sự kiện chính)
        su_kien_cols = ['ID', 'Severity', 'Start_Time', 'End_Time',
                        'Distance(mi)', 'Start_Lat', 'Start_Lng',
                        'City', 'State', 'Weather_Condition']
        su_kien = self.df[su_kien_cols].copy()

        # 7. Lưu các bảng
        vi_tri.to_csv(os.path.join(output_dir, "vi_tri.csv"), index=False)
        thoi_tiet.to_csv(os.path.join(output_dir, "thoi_tiet.csv"), index=False)
        ha_tang.to_csv(os.path.join(output_dir, "ha_tang.csv"), index=False)
        anh_sang.to_csv(os.path.join(output_dir, "anh_sang.csv"), index=False)
        thoi_gian.to_csv(os.path.join(output_dir, "thoi_gian.csv"), index=False)
        su_kien.to_csv(os.path.join(output_dir, "su_kien.csv"), index=False)

        print(f"\nĐã tách bảng và lưu tại thư mục: {output_dir}")
