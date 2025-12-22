"""
Module Transforms - Nơi Duy Nhất Chứa Logic Biến Đổi Dữ Liệu
============================================================
Tất cả các hàm biến đổi dữ liệu được định nghĩa ở đây.
Import và sử dụng ở các module khác.
KHÔNG BAO GIỜ duplicate logic ở file khác.

Quy tắc:
    - Mỗi transformation chỉ có MỘT định nghĩa
    - Tất cả ngưỡng lấy từ config.py
    - Comment rõ ràng về business logic
"""

import pandas as pd
import numpy as np
from typing import Optional

# =============================================================================
# HẰNG SỐ - Lấy từ một nguồn duy nhất
# =============================================================================

# Thời gian tai nạn
MAX_DURATION_HOURS = 24
MAX_DURATION_MIN = MAX_DURATION_HOURS * 60  # 1440 phút

# Nhiệt độ
TEMP_MIN_F = -20
TEMP_MAX_F = 120

# Tầm nhìn
VISIBILITY_MAX_MI = 10

# Mức độ nghiêm trọng
SEVERITY_MIN = 1
SEVERITY_MAX = 4
HIGH_SEVERITY_THRESHOLD = 3  # >= 3 là "nghiêm trọng"

# Trọng số hạ tầng để tính điểm rủi ro
INFRA_WEIGHTS = {
    'Junction': 3,        # Ngã tư - nguy hiểm nhất
    'Traffic_Signal': 2,  # Đèn giao thông
    'Crossing': 2,        # Đường băng qua
    'Stop': 1,            # Biển dừng
    'Amenity': 1          # Tiện ích
}

# Từ khóa phân loại thời tiết
WEATHER_KEYWORDS = {
    'Rainy': ['rain', 'drizzle', 'shower'],        # Mưa
    'Snowy': ['snow', 'ice', 'sleet', 'freezing'], # Tuyết
    'Foggy': ['fog', 'mist', 'haze'],              # Sương mù
    'Cloudy': ['cloud', 'overcast'],               # Nhiều mây
    'Windy': ['wind', 'storm', 'thunder']          # Gió bão
}

# Điểm rủi ro cơ bản theo thời tiết
WEATHER_RISK_BASE = {
    'Clear': 0,   # Trời quang
    'Cloudy': 1,  # Nhiều mây
    'Windy': 2,   # Gió
    'Rainy': 3,   # Mưa
    'Snowy': 4,   # Tuyết
    'Foggy': 5    # Sương mù - nguy hiểm nhất
}


# =============================================================================
# TÍNH THỜI GIAN TAI NẠN (Duration)
# =============================================================================

def calculate_duration(df: pd.DataFrame, 
                       start_col: str = 'Start_Time',
                       end_col: str = 'End_Time',
                       output_col: str = 'Duration_min') -> pd.DataFrame:
    """
    Tính Duration_min từ Start_Time và End_Time.
    Bao gồm cắt outlier (giá trị bất thường).
    
    SỬ DỤNG HÀM NÀY Ở MỌI NƠI cần Duration_min.
    KHÔNG tính lại Duration riêng.
    
    Tham số:
        df: DataFrame có cột thời gian
        start_col: Tên cột thời gian bắt đầu
        end_col: Tên cột thời gian kết thúc
        output_col: Tên cột output
        
    Trả về:
        DataFrame với cột Duration_min
        
    Logic:
        - Duration = End_Time - Start_Time (tính bằng phút)
        - Cắt tại 24 giờ (1440 phút) - dài hơn là lỗi dữ liệu
        - Giá trị âm đặt = 0
    """
    # Đảm bảo đúng kiểu datetime
    df[start_col] = pd.to_datetime(df[start_col])
    df[end_col] = pd.to_datetime(df[end_col])
    
    # Tính duration (đơn vị phút)
    df[output_col] = (df[end_col] - df[start_col]).dt.total_seconds() / 60
    
    # Đếm và cắt outliers
    outliers_cao = (df[output_col] > MAX_DURATION_MIN).sum()
    outliers_am = (df[output_col] < 0).sum()
    
    df.loc[df[output_col] > MAX_DURATION_MIN, output_col] = MAX_DURATION_MIN
    df.loc[df[output_col] < 0, output_col] = 0
    
    if outliers_cao > 0 or outliers_am > 0:
        print(f"  [Duration] Đã cắt {outliers_cao:,} giá trị cao, {outliers_am:,} giá trị âm")
    
    return df


def cap_duration(df: pd.DataFrame, col: str = 'Duration_min') -> pd.DataFrame:
    """
    Cắt giá trị Duration_min đã có sẵn.
    Dùng khi Duration đã tồn tại nhưng có thể có outlier.
    
    Tham số:
        df: DataFrame có cột Duration_min
        col: Tên cột duration
        
    Trả về:
        DataFrame với duration đã cắt
    """
    if col not in df.columns:
        raise ValueError(f"Không tìm thấy cột {col}. Dùng calculate_duration() thay thế.")
    
    outliers = (df[col] > MAX_DURATION_MIN).sum() + (df[col] < 0).sum()
    
    df.loc[df[col] > MAX_DURATION_MIN, col] = MAX_DURATION_MIN
    df.loc[df[col] < 0, col] = 0
    
    if outliers > 0:
        print(f"  [Duration] Đã cắt {outliers:,} outliers")
    
    return df


# =============================================================================
# PHÂN LOẠI THỜI TIẾT
# =============================================================================

def categorize_weather(condition: Optional[str]) -> str:
    """
    Phân loại điều kiện thời tiết thành nhóm đơn giản.
    
    SỬ DỤNG HÀM NÀY Ở MỌI NƠI cần phân loại thời tiết.
    
    Tham số:
        condition: Chuỗi mô tả thời tiết gốc
        
    Trả về:
        Nhóm đơn giản: Clear, Rainy, Snowy, Foggy, Cloudy, Windy
        
    Logic:
        - Mặc định 'Clear' nếu null/không rõ
        - So khớp từ khóa không phân biệt hoa thường
        - Ưu tiên: Foggy > Snowy > Rainy > Windy > Cloudy > Clear
    """
    if pd.isna(condition):
        return 'Clear'
    
    cond_lower = str(condition).lower()
    
    # Kiểm tra từng nhóm (thứ tự quan trọng)
    for category, keywords in WEATHER_KEYWORDS.items():
        if any(kw in cond_lower for kw in keywords):
            return category
    
    return 'Clear'


def add_weather_category(df: pd.DataFrame,
                         input_col: str = 'Weather_Condition',
                         output_col: str = 'Weather_Category') -> pd.DataFrame:
    """
    Thêm cột Weather_Category vào DataFrame.
    
    Tham số:
        df: DataFrame có cột thời tiết
        input_col: Tên cột thời tiết gốc
        output_col: Tên cột output
        
    Trả về:
        DataFrame với Weather_Category
    """
    df[output_col] = df[input_col].apply(categorize_weather)
    
    # In phân phối
    dist = df[output_col].value_counts()
    print(f"  [Thời tiết] Phân loại: {dict(dist)}")
    
    return df


# =============================================================================
# TÍNH ĐIỂM HẠ TẦNG
# =============================================================================

def calculate_infra_score(df: pd.DataFrame,
                          output_col: str = 'Infra_Score') -> pd.DataFrame:
    """
    Tính điểm hạ tầng từ các cột boolean.
    
    Tham số:
        df: DataFrame có các cột hạ tầng boolean
        output_col: Tên cột output
        
    Trả về:
        DataFrame với Infra_Score
        
    Logic:
        - Tổng các feature hạ tầng có mặt
        - Điểm cao = nhiều hạ tầng = có thể là đô thị
    """
    infra_cols = list(INFRA_WEIGHTS.keys())
    available = [c for c in infra_cols if c in df.columns]
    
    if not available:
        print(f"  [Hạ tầng] Cảnh báo: Không tìm thấy cột hạ tầng")
        df[output_col] = 0
        return df
    
    df[output_col] = df[available].sum(axis=1)
    
    return df


def calculate_infra_risk_score(df: pd.DataFrame,
                               output_col: str = 'Infra_Risk_Score') -> pd.DataFrame:
    """
    Tính điểm rủi ro hạ tầng có trọng số.
    
    Logic:
        - Junction nguy hiểm nhất (trọng số 3)
        - Traffic_Signal, Crossing (trọng số 2)
        - Stop, Amenity (trọng số 1)
        - Chia cho số tai nạn để normalize
    """
    weighted_sum = 0
    for col, weight in INFRA_WEIGHTS.items():
        total_col = f'total_{col.lower()}'
        if total_col in df.columns:
            weighted_sum += df[total_col] * weight
    
    if 'total_accidents' in df.columns:
        df[output_col] = (weighted_sum / df['total_accidents']).round(2)
    else:
        df[output_col] = weighted_sum
    
    return df


def classify_urban_rural(df: pd.DataFrame,
                         score_col: str = 'Infra_Score',
                         output_col: str = 'Urban_Rural') -> pd.DataFrame:
    """
    Phân loại địa điểm là Đô thị (Urban) hay Nông thôn (Rural).
    
    Logic:
        - Trên trung vị hạ tầng = Urban (đô thị)
        - Dưới trung vị = Rural (nông thôn)
    """
    if score_col not in df.columns:
        df = calculate_infra_score(df, score_col)
    
    median_score = df[score_col].median()
    df[output_col] = np.where(df[score_col] >= median_score, 'Urban', 'Rural')
    
    urban_pct = (df[output_col] == 'Urban').mean() * 100
    print(f"  [Đô thị/Nông thôn] Urban: {urban_pct:.1f}%, Rural: {100-urban_pct:.1f}%")
    
    return df


# =============================================================================
# MỨC ĐỘ NGHIÊM TRỌNG
# =============================================================================

def is_high_severity(severity: int) -> bool:
    """Kiểm tra severity có cao không (>= 3)."""
    return severity >= HIGH_SEVERITY_THRESHOLD


def add_high_severity_flag(df: pd.DataFrame,
                           input_col: str = 'Severity',
                           output_col: str = 'Is_High_Severity') -> pd.DataFrame:
    """
    Thêm cờ đánh dấu tai nạn nghiêm trọng.
    
    Trả về:
        DataFrame với Is_High_Severity (1 = nghiêm trọng, 0 = không)
    """
    df[output_col] = (df[input_col] >= HIGH_SEVERITY_THRESHOLD).astype(int)
    
    high_pct = df[output_col].mean() * 100
    print(f"  [Severity] Tỷ lệ nghiêm trọng: {high_pct:.1f}%")
    
    return df


# =============================================================================
# XỬ LÝ THỜI GIAN
# =============================================================================

def get_time_period(hour: int) -> str:
    """
    Phân loại giờ thành khoảng thời gian trong ngày.
    
    Tham số:
        hour: Giờ trong ngày (0-23)
        
    Trả về:
        Tên khoảng thời gian
    """
    if 6 <= hour < 9:
        return 'Morning_Rush'    # Giờ cao điểm sáng
    elif 9 <= hour < 12:
        return 'Late_Morning'    # Cuối buổi sáng
    elif 12 <= hour < 14:
        return 'Lunch'           # Giờ trưa
    elif 14 <= hour < 17:
        return 'Afternoon'       # Chiều
    elif 17 <= hour < 20:
        return 'Evening_Rush'    # Giờ cao điểm chiều
    else:
        return 'Night'           # Đêm


def is_rush_hour(hour: int) -> bool:
    """Kiểm tra có phải giờ cao điểm không (7-9 sáng hoặc 4-6 chiều)."""
    return hour in [7, 8, 9, 16, 17, 18]


def add_time_features(df: pd.DataFrame,
                      time_col: str = 'Start_Time') -> pd.DataFrame:
    """
    Thêm tất cả các feature liên quan đến thời gian.
    
    Trả về:
        DataFrame với các cột:
        - Year, Month, Day, Hour, DayOfWeek, DayName, Quarter
        - Time_Period, Is_Rush_Hour, Is_Weekend
    """
    df[time_col] = pd.to_datetime(df[time_col])
    
    df['Year'] = df[time_col].dt.year          # Năm
    df['Month'] = df[time_col].dt.month        # Tháng
    df['Day'] = df[time_col].dt.day            # Ngày
    df['Hour'] = df[time_col].dt.hour          # Giờ
    df['DayOfWeek'] = df[time_col].dt.dayofweek  # Thứ (0=T2, 6=CN)
    df['DayName'] = df[time_col].dt.day_name()   # Tên thứ
    df['Quarter'] = df[time_col].dt.quarter      # Quý
    
    df['Time_Period'] = df['Hour'].apply(get_time_period)
    df['Is_Rush_Hour'] = df['Hour'].apply(lambda x: 1 if is_rush_hour(x) else 0)
    df['Is_Weekend'] = (df['DayOfWeek'] >= 5).astype(int)  # T7, CN = weekend
    
    print(f"  [Thời gian] Đã thêm 10 features thời gian")
    
    return df


# =============================================================================
# TÍNH TOÁN CHỈ SỐ
# =============================================================================

def calculate_pct_change(current: float, previous: float) -> float:
    """
    Tính phần trăm thay đổi.
    
    Công thức: (current - previous) / previous * 100
    
    Trả về:
        Phần trăm thay đổi, hoặc NaN nếu previous = 0
    """
    if previous == 0 or pd.isna(previous):
        return np.nan
    return ((current - previous) / previous) * 100


def calculate_z_score(value: float, mean: float, std: float) -> float:
    """
    Tính Z-score (điểm chuẩn hóa).
    
    Công thức: (value - mean) / std
    
    Ý nghĩa:
        - Z > 2: Rất bất thường (2 độ lệch chuẩn trên TB)
        - Z > 1: Khá bất thường
        - Z ~ 0: Gần trung bình
        - Z < 0: Dưới trung bình
    """
    if std == 0 or pd.isna(std):
        return 0
    return (value - mean) / std


def calculate_impact_pct(value: float, baseline: float) -> float:
    """
    Tính phần trăm tác động so với baseline.
    
    Công thức: (value - baseline) / baseline * 100
    
    Ví dụ:
        - Severity trời mưa = 2.5
        - Baseline (trời quang) = 2.3
        - Impact = (2.5 - 2.3) / 2.3 * 100 = 8.7%
        - Nghĩa là: Mưa làm tăng severity 8.7% so với trời quang
    
    Trả về:
        Phần trăm tác động (dương = tệ hơn baseline)
    """
    if baseline == 0 or pd.isna(baseline):
        return 0
    return ((value - baseline) / baseline) * 100


# =============================================================================
# TÍNH ĐIỂM RỦI RO
# =============================================================================

def calculate_weather_risk_score(weather_category: str,
                                 severity_increase_pct: float,
                                 duration_increase_pct: float) -> int:
    """
    Tính điểm rủi ro thời tiết tổng hợp (thang 0-10).
    
    Tham số:
        weather_category: Nhóm thời tiết
        severity_increase_pct: % tăng severity so với Clear
        duration_increase_pct: % tăng duration so với Clear
        
    Trả về:
        Điểm rủi ro 0-10
        
    Logic:
        - Điểm cơ bản từ loại thời tiết (Foggy=5, Snowy=4, ...)
        - +1 đến +3 nếu severity tăng nhiều
        - +1 đến +2 nếu duration tăng nhiều
        - Tối đa 10
    """
    # Điểm cơ bản từ loại thời tiết
    score = WEATHER_RISK_BASE.get(weather_category, 0)
    
    # Cộng thêm dựa trên severity
    if severity_increase_pct > 15:
        score += 3
    elif severity_increase_pct > 10:
        score += 2
    elif severity_increase_pct > 5:
        score += 1
    
    # Cộng thêm dựa trên duration
    if duration_increase_pct > 30:
        score += 2
    elif duration_increase_pct > 15:
        score += 1
    
    return min(score, 10)  # Tối đa 10


def categorize_risk(score: float, 
                    thresholds: tuple = (8, 6, 4)) -> str:
    """
    Phân loại mức rủi ro từ điểm số.
    
    Tham số:
        score: Điểm rủi ro
        thresholds: (ngưỡng_extreme, ngưỡng_high, ngưỡng_moderate)
        
    Trả về:
        Mức rủi ro: Extreme, High, Moderate, hoặc Low
    """
    extreme_t, high_t, moderate_t = thresholds
    
    if score >= extreme_t:
        return 'Extreme'    # Cực kỳ nguy hiểm
    elif score >= high_t:
        return 'High'       # Nguy hiểm cao
    elif score >= moderate_t:
        return 'Moderate'   # Trung bình
    return 'Low'            # Thấp


def categorize_anomaly(z_score: float) -> str:
    """
    Phân loại mức độ bất thường từ Z-score.
    
    Trả về:
        - Critical: Z > 2 (rất bất thường)
        - High: Z > 1
        - Elevated: Z > 0
        - Normal: Z <= 0
    """
    if pd.isna(z_score):
        return 'Normal'
    if z_score > 2:
        return 'Critical'   # Nghiêm trọng
    if z_score > 1:
        return 'High'       # Cao
    if z_score > 0:
        return 'Elevated'   # Tăng cao
    return 'Normal'         # Bình thường


# =============================================================================
# TEST MODULE
# =============================================================================

if __name__ == "__main__":
    print("Module Transforms - Nơi Duy Nhất Chứa Logic Biến Đổi")
    print("=" * 50)
    print("\nHằng số:")
    print(f"  MAX_DURATION_MIN: {MAX_DURATION_MIN} phút")
    print(f"  HIGH_SEVERITY_THRESHOLD: {HIGH_SEVERITY_THRESHOLD}")
    print(f"  INFRA_WEIGHTS: {INFRA_WEIGHTS}")
    
    print("\nCác hàm có sẵn:")
    print("  Duration: calculate_duration(), cap_duration()")
    print("  Thời tiết: categorize_weather(), add_weather_category()")
    print("  Hạ tầng: calculate_infra_score(), classify_urban_rural()")
    print("  Severity: is_high_severity(), add_high_severity_flag()")
    print("  Thời gian: add_time_features(), get_time_period()")
    print("  Chỉ số: calculate_pct_change(), calculate_z_score()")
    print("  Rủi ro: calculate_weather_risk_score(), categorize_risk()")