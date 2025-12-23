"""
Module Tạo Star Schema (ĐÃ SỬA BUG)
===================================
Tạo star schema từ dữ liệu đã làm sạch.

SỬA LỖI QUAN TRỌNG: Duration_min giờ được CẮT sau khi tính lại.
Bug cũ: Duration được tính lại KHÔNG cắt, gây ra giá trị impact 35,000%.

Schema:
    - dim_time: Date (PK), Day, Month, Quarter, Year
    - dim_location: Location_id (PK), Street, City, County, State, Timezone, infra counts
    - dim_weather: weather_id (PK), Weather_Condition
    - accident_detail: ID, Location_id (FK), weather_id (FK), full_date (FK),
                       Start_Time, End_Time, Severity, Duration_min, Description
"""

import pandas as pd
import numpy as np
import os
import time
import config
from transforms import cap_duration, MAX_DURATION_MIN
from validators import (
    validate_stage, validate_referential_integrity, 
    quick_sanity_check, assert_no_extreme_values,
    SPLITTER_RULES
)


def safe_to_csv(df: pd.DataFrame, output_path, max_retries: int = None, retry_delay: int = None) -> bool:
    """
    Lưu DataFrame ra CSV với retry nếu file bị khóa.
    
    Tham số:
        df: DataFrame cần lưu
        output_path: Đường dẫn file
        max_retries: Số lần thử lại tối đa
        retry_delay: Số giây chờ giữa các lần thử
    
    Trả về:
        True nếu thành công
    """
    max_retries = max_retries or config.FILE_MAX_RETRIES
    retry_delay = retry_delay or config.FILE_RETRY_DELAY
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            if output_path.exists():
                try:
                    os.remove(output_path)
                except PermissionError:
                    pass
            
            df.to_csv(output_path, index=False)
            return True
            
        except PermissionError as e:
            if attempt < max_retries - 1:
                print(f"  Cảnh báo: File bị khóa. Thử lại {attempt + 1}/{max_retries} sau {retry_delay}s...")
                print(f"  Vui lòng đóng file nếu đang mở trong Excel.")
                time.sleep(retry_delay)
            else:
                print(f"\n  LỖI: Không thể ghi vào {output_path}")
                print(f"  File đang được sử dụng bởi chương trình khác.")
                raise e
    
    return False


class DataSplitter:
    """
    Tạo star schema từ dữ liệu đã làm sạch.
    
    QUAN TRỌNG: Validate tất cả output và kiểm tra referential integrity.
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.dim_time = None
        self.dim_location = None
        self.dim_weather = None
        self.fact = None
        
    def create_dim_time(self) -> pd.DataFrame:
        """
        Tạo bảng dimension thời gian.
        
        PK: Date (chỉ ngày, không có giờ)
        Các cột: Date, Day, Month, Quarter, Year
        """
        print("\n[Splitter] Đang tạo dim_time...")
        
        # Đảm bảo datetime
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'])
        
        # Trích xuất các thành phần ngày
        self.df['Date'] = self.df['Start_Time'].dt.date
        self.df['Day'] = self.df['Start_Time'].dt.day
        self.df['Month'] = self.df['Start_Time'].dt.month
        self.df['Quarter'] = self.df['Start_Time'].dt.quarter
        self.df['Year'] = self.df['Start_Time'].dt.year
        
        # Tạo dimension
        time_cols = ['Date', 'Day', 'Month', 'Quarter', 'Year']
        dim_time = self.df[time_cols].drop_duplicates().reset_index(drop=True)
        dim_time = dim_time.sort_values('Date').reset_index(drop=True)
        
        # Validate
        assert dim_time['Date'].nunique() == len(dim_time), "dim_time Date không duy nhất!"
        
        # Lưu
        output_path = config.DIM_DIR / "dim_time.csv"
        safe_to_csv(dim_time, output_path)
        
        print(f"  Số bản ghi: {len(dim_time):,}")
        print(f"  PK: Date (duy nhất: ĐÃ XÁC NHẬN)")
        print(f"  Khoảng: {dim_time['Date'].min()} đến {dim_time['Date'].max()}")
        
        self.dim_time = dim_time
        return dim_time
    
    def create_dim_location(self) -> pd.DataFrame:
        """
        Tạo bảng dimension địa điểm.
        
        PK: Location_id (Street + "_" + City) - PHẢI DUY NHẤT
        """
        print("\n[Splitter] Đang tạo dim_location...")
        
        # Tạo Location_id
        self.df['Location_id'] = (
            self.df['Street'].astype(str) + "_" + 
            self.df['City'].astype(str)
        )
        
        # Các cột cần aggregate
        location_cols = ['Street', 'City', 'County', 'State', 'Timezone']
        bool_cols = config.INFRA_COLUMNS
        
        available_location = [c for c in location_cols if c in self.df.columns]
        available_bool = [c for c in bool_cols if c in self.df.columns]
        
        # Xây dựng aggregation: cột địa điểm = first, cột boolean = sum
        agg_dict = {col: 'first' for col in available_location}
        for col in available_bool:
            agg_dict[col] = lambda x, c=col: (x == True).sum()
        
        # Group by Location_id để đảm bảo duy nhất
        dim_location = self.df.groupby('Location_id', dropna=False).agg(agg_dict).reset_index()
        dim_location = dim_location.sort_values(['State', 'City']).reset_index(drop=True)
        
        # Validate tính duy nhất
        n_unique = dim_location['Location_id'].nunique()
        n_total = len(dim_location)
        assert n_unique == n_total, f"Location_id không duy nhất! {n_unique} vs {n_total}"
        
        # Lưu
        output_path = config.DIM_DIR / "dim_location.csv"
        safe_to_csv(dim_location, output_path)
        
        print(f"  Số bản ghi: {len(dim_location):,}")
        print(f"  PK: Location_id (duy nhất: ĐÃ XÁC NHẬN)")
        print(f"  Số bang: {dim_location['State'].nunique()}")
        
        self.dim_location = dim_location
        return dim_location
    
    def create_dim_weather(self) -> pd.DataFrame:
        """
        Tạo bảng dimension thời tiết.
        
        PK: weather_id (W1, W2, ...)
        """
        print("\n[Splitter] Đang tạo dim_weather...")
        
        dim_weather = self.df[['Weather_Condition']].drop_duplicates().reset_index(drop=True)
        dim_weather['weather_id'] = ["W" + str(i+1) for i in range(len(dim_weather))]
        dim_weather = dim_weather[['weather_id', 'Weather_Condition']]
        
        # Validate
        assert dim_weather['weather_id'].nunique() == len(dim_weather), "weather_id không duy nhất!"
        
        # Lưu
        output_path = config.DIM_DIR / "dim_weather.csv"
        safe_to_csv(dim_weather, output_path)
        
        print(f"  Số bản ghi: {len(dim_weather):,}")
        print(f"  PK: weather_id (duy nhất: ĐÃ XÁC NHẬN)")
        
        self.dim_weather = dim_weather
        return dim_weather
    
    def create_fact_table(self) -> pd.DataFrame:
        """
        Tạo bảng fact accident_detail.
        
        SỬA LỖI QUAN TRỌNG: Duration_min được CẮT sau khi tính lại.
        
        Keys:
            - ID: Khóa chính
            - Location_id: FK -> dim_location
            - weather_id: FK -> dim_weather  
            - full_date: FK -> dim_time.Date
        
        Measures:
            - Severity, Duration_min (ĐÃ CẮT!)
        """
        print("\n[Splitter] Đang tạo accident_detail...")
        
        fact = self.df.copy()
        
        # Map weather_id
        fact = fact.merge(self.dim_weather, on='Weather_Condition', how='left')
        
        # Tạo Location_id (nếu chưa có)
        if 'Location_id' not in fact.columns:
            fact['Location_id'] = (
                fact['Street'].astype(str) + "_" + 
                fact['City'].astype(str)
            )
        
        # Tạo full_date
        fact['Start_Time'] = pd.to_datetime(fact['Start_Time'])
        fact['End_Time'] = pd.to_datetime(fact['End_Time'])
        fact['full_date'] = fact['Start_Time'].dt.date
        
        # ================================================================
        # SỬA LỖI QUAN TRỌNG: Tính Duration_min CÓ CẮT
        # ================================================================
        # Bug cũ: Duration được tính lại KHÔNG cắt
        # Kết quả: Duration = 51,118 phút (35 ngày!) → impact = 35,859%
        
        fact['Duration_min'] = (
            (fact['End_Time'] - fact['Start_Time']).dt.total_seconds() / 60
        )
        
        # CẮT OUTLIERS - TRƯỚC ĐÂY BỊ THIẾU!
        outliers_cao = (fact['Duration_min'] > MAX_DURATION_MIN).sum()
        outliers_am = (fact['Duration_min'] < 0).sum()
        
        fact.loc[fact['Duration_min'] > MAX_DURATION_MIN, 'Duration_min'] = MAX_DURATION_MIN
        fact.loc[fact['Duration_min'] < 0, 'Duration_min'] = 0
        
        print(f"  [SỬA LỖI] Đã cắt Duration outliers: {outliers_cao:,} cao, {outliers_am:,} âm")
        
        # Validate Duration đã được cắt
        actual_max = fact['Duration_min'].max()
        assert actual_max <= MAX_DURATION_MIN, \
            f"NGHIÊM TRỌNG: Duration_min chưa được cắt! Max={actual_max} > {MAX_DURATION_MIN}"
        print(f"  Duration_min max: {actual_max:.1f} (giới hạn: {MAX_DURATION_MIN}) ✓")
        # ================================================================
        
        # Chọn các cột fact
        fact_cols = [
            'ID',               # PK
            'Location_id',      # FK -> dim_location
            'weather_id',       # FK -> dim_weather
            'full_date',        # FK -> dim_time.Date
            'Start_Time',       # Timestamp đầy đủ
            'End_Time',         # Timestamp đầy đủ
            'Severity',         # Measure
            'Duration_min',     # Measure (ĐÃ CẮT!)
            'Description'       # Attribute
        ]
        
        available_cols = [c for c in fact_cols if c in fact.columns]
        accident_detail = fact[available_cols].copy()
        
        # Lưu
        output_path = config.FACT_DIR / "accident_detail.csv"
        safe_to_csv(accident_detail, output_path)
        
        print(f"  Số bản ghi: {len(accident_detail):,}")
        print(f"  PK: ID")
        print(f"  FKs: Location_id, weather_id, full_date")
        
        self.fact = accident_detail
        return accident_detail
    
    def validate_schema(self) -> bool:
        """
        Validate referential integrity giữa fact và dimension.
        
        Trả về:
            True nếu tất cả validation pass
        """
        print("\n[Splitter] Đang validate schema integrity...")
        
        all_valid = True
        
        # Kiểm tra fact.full_date -> dim_time.Date
        dim_dates = set(pd.to_datetime(self.dim_time['Date']).dt.date)
        fact_dates = set(pd.to_datetime(self.fact['full_date']).dt.date)
        missing_dates = len(fact_dates - dim_dates)
        
        # Kiểm tra fact.Location_id -> dim_location.Location_id
        missing_locations = self.fact[
            ~self.fact['Location_id'].isin(self.dim_location['Location_id'])
        ].shape[0]
        
        # Kiểm tra fact.weather_id -> dim_weather.weather_id
        missing_weather = self.fact[
            ~self.fact['weather_id'].isin(self.dim_weather['weather_id'])
        ].shape[0]
        
        # Báo cáo
        print(f"  full_date orphans: {missing_dates}")
        print(f"  Location_id orphans: {missing_locations}")
        print(f"  weather_id orphans: {missing_weather}")
        
        if missing_dates + missing_locations + missing_weather == 0:
            print("  Schema validation: THÀNH CÔNG ✓")
        else:
            print("  Schema validation: CẢNH BÁO - có bản ghi mồ côi!")
            all_valid = False
        
        # Validation bổ sung: Kiểm tra Duration_min đã được cắt
        max_duration = self.fact['Duration_min'].max()
        if max_duration > MAX_DURATION_MIN:
            print(f"  NGHIÊM TRỌNG: Duration_min chưa được cắt! Max={max_duration}")
            all_valid = False
        else:
            print(f"  Duration_min cap: ĐÃ XÁC NHẬN (max={max_duration:.1f})")
        
        return all_valid
    
    def run_all(self) -> tuple:
        """
        Tạo star schema hoàn chỉnh với validation.
        
        Trả về:
            Tuple (dim_time, dim_location, dim_weather, fact)
        """
        print("\n" + "="*60)
        print("TẠO STAR SCHEMA")
        print("="*60)
        print(f"Input: {len(self.df):,} bản ghi")
        
        # Tạo dimensions
        self.create_dim_time()
        self.create_dim_location()
        self.create_dim_weather()
        
        # Tạo fact table (có sửa Duration)
        self.create_fact_table()
        
        # Validate
        is_valid = self.validate_schema()
        
        # Tổng kết
        print("\n" + "-"*60)
        print("TÓM TẮT SCHEMA")
        print("-"*60)
        print(f"  dim_time:        {len(self.dim_time):>10,} bản ghi  (PK: Date)")
        print(f"  dim_location:    {len(self.dim_location):>10,} bản ghi  (PK: Location_id)")
        print(f"  dim_weather:     {len(self.dim_weather):>10,} bản ghi  (PK: weather_id)")
        print(f"  accident_detail: {len(self.fact):>10,} bản ghi  (PK: ID)")
        print(f"\n  Validation: {'THÀNH CÔNG' if is_valid else 'THẤT BẠI'}")
        
        # Kiểm tra nhanh
        quick_sanity_check(self.fact, 'fact_table')
        
        return self.dim_time, self.dim_location, self.dim_weather, self.fact