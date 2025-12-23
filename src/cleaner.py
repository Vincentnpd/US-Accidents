"""
Module Làm Sạch Dữ Liệu
=======================
Xử lý missing values, outliers, và tạo features.
Sử dụng transforms.py cho tất cả transformations (Nguồn Duy Nhất).
Validate output trước khi lưu.
"""

import pandas as pd
import numpy as np
import config
from transforms import (
    calculate_duration, cap_duration,
    add_weather_category, add_high_severity_flag,
    add_time_features, calculate_infra_score,
    MAX_DURATION_MIN
)
from validators import validate_stage, quick_sanity_check, CLEANER_RULES


class DataCleaner:
    """
    Làm sạch dữ liệu và tạo features.
    
    Pipeline:
        1. Xử lý missing values (giá trị thiếu)
        2. Xử lý outliers (giá trị bất thường)
        3. Tạo time features
        4. Tạo weather features
        5. Tạo infrastructure features
        6. Validate output
        7. Lưu file
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.initial_count = len(df)
        
    def handle_missing(self) -> None:
        """
        Xử lý missing values theo chiến lược phù hợp.
        
        Chiến lược:
            - Số: Fill với median (robust với outliers)
            - Boolean: Fill với False (giả sử feature không có)
            - Categorical: Fill với mode hoặc 'Unknown'
        """
        print("\n[Cleaner] Đang xử lý missing values...")
        
        # Cột số - fill với median
        numeric_cols = ['Temperature(F)', 'Visibility(mi)', 'Precipitation(in)']
        for col in numeric_cols:
            if col in self.df.columns:
                missing_count = self.df[col].isna().sum()
                if missing_count > 0:
                    median_val = self.df[col].median()
                    self.df[col] = self.df[col].fillna(median_val)
                    print(f"  {col}: fill {missing_count:,} với median ({median_val:.2f})")
        
        # Cột boolean - fill với False
        bool_cols = config.INFRA_COLUMNS + ['Railway', 'Station']
        for col in bool_cols:
            if col in self.df.columns:
                missing_count = self.df[col].isna().sum()
                if missing_count > 0:
                    self.df[col] = self.df[col].fillna(False)
                    print(f"  {col}: fill {missing_count:,} với False")
        
        # Điều kiện thời tiết - fill với 'Clear' (trời quang)
        if 'Weather_Condition' in self.df.columns:
            missing_count = self.df['Weather_Condition'].isna().sum()
            if missing_count > 0:
                self.df['Weather_Condition'] = self.df['Weather_Condition'].fillna('Clear')
                print(f"  Weather_Condition: fill {missing_count:,} với 'Clear'")
        
        # Cột địa điểm - fill với 'Unknown'
        location_cols = ['City', 'County', 'Street']
        for col in location_cols:
            if col in self.df.columns:
                missing_count = self.df[col].isna().sum()
                if missing_count > 0:
                    self.df[col] = self.df[col].fillna('Unknown')
                    print(f"  {col}: fill {missing_count:,} với 'Unknown'")
        
        print("  Đã xử lý xong missing values.")
        
    def handle_outliers(self) -> None:
        """
        Xử lý outliers bằng cắt (capping/winsorization).
        
        Dùng chiến lược cắt thay vì xóa để giữ số lượng bản ghi.
        """
        print("\n[Cleaner] Đang xử lý outliers...")
        
        # Nhiệt độ
        if 'Temperature(F)' in self.df.columns:
            low = (self.df['Temperature(F)'] < config.TEMP_MIN).sum()
            high = (self.df['Temperature(F)'] > config.TEMP_MAX).sum()
            self.df.loc[self.df['Temperature(F)'] < config.TEMP_MIN, 'Temperature(F)'] = config.TEMP_MIN
            self.df.loc[self.df['Temperature(F)'] > config.TEMP_MAX, 'Temperature(F)'] = config.TEMP_MAX
            if low + high > 0:
                print(f"  Temperature(F): cắt {low + high:,} outliers ({config.TEMP_MIN} đến {config.TEMP_MAX})")
        
        # Tầm nhìn
        if 'Visibility(mi)' in self.df.columns:
            low = (self.df['Visibility(mi)'] < config.VISIBILITY_MIN).sum()
            high = (self.df['Visibility(mi)'] > config.VISIBILITY_MAX).sum()
            self.df.loc[self.df['Visibility(mi)'] < config.VISIBILITY_MIN, 'Visibility(mi)'] = config.VISIBILITY_MIN
            self.df.loc[self.df['Visibility(mi)'] > config.VISIBILITY_MAX, 'Visibility(mi)'] = config.VISIBILITY_MAX
            if low + high > 0:
                print(f"  Visibility(mi): cắt {low + high:,} outliers ({config.VISIBILITY_MIN} đến {config.VISIBILITY_MAX})")
        
        # Severity - XÓA bản ghi không hợp lệ (không cắt)
        if 'Severity' in self.df.columns:
            invalid = ~self.df['Severity'].isin([1, 2, 3, 4])
            invalid_count = invalid.sum()
            if invalid_count > 0:
                self.df = self.df[~invalid].copy()
                print(f"  Severity: ĐÃ XÓA {invalid_count:,} bản ghi không hợp lệ (không phải 1-4)")
        
        print("  Đã xử lý xong outliers.")
        
    def create_duration_feature(self) -> None:
        """
        Tạo Duration_min với cắt outlier.
        
        QUAN TRỌNG: Dùng transforms.calculate_duration() để đảm bảo
        logic nhất quán trong toàn pipeline.
        """
        print("\n[Cleaner] Đang tạo Duration_min...")
        
        # Đảm bảo datetime
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'])
        self.df['End_Time'] = pd.to_datetime(self.df['End_Time'])
        
        # Tính và cắt dùng hàm tập trung
        self.df = calculate_duration(self.df, 'Start_Time', 'End_Time', 'Duration_min')
        
        # Validate
        actual_max = self.df['Duration_min'].max()
        assert actual_max <= MAX_DURATION_MIN, \
            f"Duration_min chưa được cắt! Max={actual_max} > {MAX_DURATION_MIN}"
        
        print(f"  Duration_min: {self.df['Duration_min'].min():.1f} đến {actual_max:.1f} phút")
        
    def create_time_features(self) -> None:
        """Tạo các features liên quan thời gian."""
        print("\n[Cleaner] Đang tạo time features...")
        
        self.df = add_time_features(self.df, 'Start_Time')
        
    def create_weather_features(self) -> None:
        """Tạo các features liên quan thời tiết."""
        print("\n[Cleaner] Đang tạo weather features...")
        
        # Phân loại thời tiết
        if 'Weather_Condition' in self.df.columns:
            self.df = add_weather_category(self.df, 'Weather_Condition', 'Weather_Category')
        
        # Cờ nhị phân cho từng loại thời tiết
        if 'Weather_Condition' in self.df.columns:
            self.df['Is_Rain'] = self.df['Weather_Condition'].str.contains(
                'Rain|Drizzle', case=False, na=False
            ).astype(int)
            self.df['Is_Snow'] = self.df['Weather_Condition'].str.contains(
                'Snow|Ice|Sleet', case=False, na=False
            ).astype(int)
            self.df['Is_Fog'] = self.df['Weather_Condition'].str.contains(
                'Fog|Mist|Haze', case=False, na=False
            ).astype(int)
            print("  Đã tạo cờ: Is_Rain, Is_Snow, Is_Fog")
        
        # Cờ tầm nhìn thấp
        if 'Visibility(mi)' in self.df.columns:
            self.df['Low_Visibility'] = (self.df['Visibility(mi)'] < 1).astype(int)
            low_vis_pct = self.df['Low_Visibility'].mean() * 100
            print(f"  Low_Visibility: {low_vis_pct:.1f}% bản ghi")
        
    def create_infrastructure_features(self) -> None:
        """Tạo features liên quan hạ tầng."""
        print("\n[Cleaner] Đang tạo infrastructure features...")
        
        self.df = calculate_infra_score(self.df, 'Infra_Score')
        
        print(f"  Infra_Score: {self.df['Infra_Score'].min()} đến {self.df['Infra_Score'].max()}")
        
    def create_severity_features(self) -> None:
        """Tạo features liên quan mức độ nghiêm trọng."""
        print("\n[Cleaner] Đang tạo severity features...")
        
        self.df = add_high_severity_flag(self.df, 'Severity', 'Is_High_Severity')
        
    def create_location_id(self) -> None:
        """Tạo ID địa điểm cho bảng dimension."""
        print("\n[Cleaner] Đang tạo Location_ID...")
        
        self.df['Location_ID'] = (
            self.df['Street'].astype(str) + '_' + 
            self.df['City'].astype(str)
        )
        
        n_unique = self.df['Location_ID'].nunique()
        print(f"  Location_ID: {n_unique:,} địa điểm duy nhất")
        
    def validate_output(self) -> None:
        """
        Validate dữ liệu đã làm sạch trước khi lưu.
        
        DỪNG NGAY: Raise error nếu validation thất bại.
        """
        print("\n[Cleaner] Đang validate output...")
        
        # Quy tắc validation
        rules = {
            'Severity': {'min': 1, 'max': 4, 'null_pct_max': 0},
            'Duration_min': {'min': 0, 'max': config.DURATION_MAX_MIN, 'null_pct_max': 0.05},
            'Year': {'min': config.START_YEAR, 'max': config.END_YEAR},
        }
        
        if 'Temperature(F)' in self.df.columns:
            rules['Temperature(F)'] = {'min': config.TEMP_MIN, 'max': config.TEMP_MAX}
        if 'Visibility(mi)' in self.df.columns:
            rules['Visibility(mi)'] = {'min': config.VISIBILITY_MIN, 'max': config.VISIBILITY_MAX}
        
        validate_stage(self.df, 'cleaner', rules, raise_on_fail=True)
        
    def save_cleaned_data(self) -> None:
        """Lưu file dữ liệu đã làm sạch."""
        output_path = config.CLEANED_DIR / "accidents_cleaned.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"\n[Cleaner] Đang lưu vào {output_path.name}...")
        self.df.to_csv(output_path, index=False)
        
        file_size = output_path.stat().st_size / (1024**2)
        print(f"  Đã lưu: {len(self.df):,} bản ghi ({file_size:.1f} MB)")
        
    def run_all(self) -> pd.DataFrame:
        """
        Chạy toàn bộ pipeline làm sạch.
        
        Các bước:
            1. Xử lý missing values
            2. Xử lý outliers
            3. Tạo Duration (có cắt)
            4. Tạo time features
            5. Tạo weather features
            6. Tạo infrastructure features
            7. Tạo severity features
            8. Tạo location ID
            9. Validate output
            10. Lưu file
        
        Trả về:
            DataFrame đã làm sạch
        """
        print("\n" + "="*60)
        print("PIPELINE LÀM SẠCH DỮ LIỆU")
        print("="*60)
        print(f"Input: {self.initial_count:,} bản ghi")
        
        # Bước 1-2: Chất lượng dữ liệu
        self.handle_missing()
        self.handle_outliers()
        
        # Bước 3-8: Tạo features
        self.create_duration_feature()
        self.create_time_features()
        self.create_weather_features()
        self.create_infrastructure_features()
        self.create_severity_features()
        self.create_location_id()
        
        # Bước 9-10: Validate và lưu
        self.validate_output()
        self.save_cleaned_data()
        
        # Tổng kết
        final_count = len(self.df)
        removed = self.initial_count - final_count
        print("\n" + "-"*60)
        print("HOÀN THÀNH LÀM SẠCH")
        print("-"*60)
        print(f"  Input:   {self.initial_count:,} bản ghi")
        print(f"  Output:  {final_count:,} bản ghi")
        print(f"  Đã xóa:  {removed:,} bản ghi ({removed/self.initial_count*100:.2f}%)")
        
        # Kiểm tra nhanh
        quick_sanity_check(self.df, 'cleaner_output')
        
        return self.df