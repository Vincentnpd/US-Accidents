import pandas as pd
import os

class DataSplitter:
    """
    Tách dữ liệu tai nạn giao thông thành các bảng quan hệ nhỏ:
    - su_kien (sự kiện chính)
    - vi_tri (City, State)
    - thoi_tiet (Weather_Condition)
    - anh_sang (Sunrise_Sunset)
    - thoi_gian (Start_Time, End_Time)
    - ha_tang (Street, Crossing, Station, Junction)
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def split_and_save(self, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)

        # Bảng vị trí
        vi_tri = self.df[['City', 'State']].drop_duplicates().reset_index(drop=True)
        vi_tri['Location_ID'] = range(1, len(vi_tri) + 1)
        vi_tri.to_csv(os.path.join(output_dir, 'vi_tri.csv'), index=False)

        # Bảng thời tiết
        thoi_tiet = self.df[['Weather_Condition']].drop_duplicates().reset_index(drop=True)
        thoi_tiet['Weather_ID'] = range(1, len(thoi_tiet) + 1)
        thoi_tiet.to_csv(os.path.join(output_dir, 'thoi_tiet.csv'), index=False)

        # Bảng ánh sáng
        anh_sang = self.df[['Sunrise_Sunset']].drop_duplicates().reset_index(drop=True)
        anh_sang['Light_ID'] = range(1, len(anh_sang) + 1)
        anh_sang.to_csv(os.path.join(output_dir, 'anh_sang.csv'), index=False)

        # Bảng thời gian
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'], errors='coerce')
        self.df['End_Time'] = pd.to_datetime(self.df['End_Time'], errors='coerce')

        thoi_gian = self.df[['Start_Time', 'End_Time']].drop_duplicates().reset_index(drop=True)
        thoi_gian['Time_ID'] = range(1, len(thoi_gian) + 1)
        thoi_gian['Date'] = thoi_gian['Start_Time'].dt.date
        thoi_gian['Year'] = thoi_gian['Start_Time'].dt.year
        thoi_gian['Month'] = thoi_gian['Start_Time'].dt.month
        thoi_gian['Day'] = thoi_gian['Start_Time'].dt.day
        thoi_gian['Hour'] = thoi_gian['Start_Time'].dt.hour
        thoi_gian.to_csv(os.path.join(output_dir, 'thoi_gian.csv'), index=False)

        # Bảng hạ tầng
        ha_tang = self.df[['Street', 'Crossing', 'Station', 'Junction']].drop_duplicates().reset_index(drop=True)
        ha_tang['Infra_ID'] = range(1, len(ha_tang) + 1)
        ha_tang.to_csv(os.path.join(output_dir, 'ha_tang.csv'), index=False)

        # Bảng sự kiện chính
        su_kien = self.df[['ID', 'Severity', 'Distance(mi)', 'City', 'State',
                           'Weather_Condition', 'Sunrise_Sunset', 'Start_Time', 'End_Time',
                           'Street', 'Crossing', 'Station', 'Junction']].copy()

        su_kien['Location_ID'] = su_kien.merge(vi_tri, on=['City', 'State'], how='left')['Location_ID']
        su_kien['Weather_ID'] = su_kien.merge(thoi_tiet, on='Weather_Condition', how='left')['Weather_ID']
        su_kien['Light_ID'] = su_kien.merge(anh_sang, on='Sunrise_Sunset', how='left')['Light_ID']
        su_kien['Time_ID'] = su_kien.merge(thoi_gian, on=['Start_Time', 'End_Time'], how='left')['Time_ID']
        su_kien['Infra_ID'] = su_kien.merge(ha_tang, on=['Street', 'Crossing', 'Station', 'Junction'], how='left')['Infra_ID']

        su_kien = su_kien[['ID', 'Severity', 'Distance(mi)', 'Location_ID',
                           'Weather_ID', 'Light_ID', 'Infra_ID', 'Time_ID']]

        # Thêm dòng này để tạo ID dạng 1, 2, 3, 4, ...
        su_kien['SuKien_ID'] = range(1, len(su_kien) + 1)

        su_kien = su_kien[['SuKien_ID', 'Severity', 'Distance(mi)', 
                   'Location_ID', 'Weather_ID', 'Light_ID', 
                   'Infra_ID', 'Time_ID']]


        su_kien.to_csv(os.path.join(output_dir, 'su_kien.csv'), index=False)

        print("Đã tách và lưu các bảng thành công:")
        print("- vi_tri.csv")
        print("- thoi_tiet.csv")
        print("- anh_sang.csv")
        print("- thoi_gian.csv")
        print("- ha_tang.csv")
        print("- su_kien.csv")
