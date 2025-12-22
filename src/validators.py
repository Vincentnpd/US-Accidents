"""
Module Validators - Kiểm Tra Chất Lượng Dữ Liệu
===============================================
Validate dữ liệu ở MỖI bước pipeline.
Fail fast (dừng ngay) nếu phát hiện vấn đề.

Cách dùng:
    from validators import validate_stage, ValidationError
    
    # Cuối mỗi bước pipeline:
    validate_stage(df, 'cleaner', CLEANER_RULES)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


# =============================================================================
# QUY TẮC VALIDATION THEO TỪNG BƯỚC
# =============================================================================

# Format: {tên_cột: {loại_check: giá_trị}}
# Các loại check: min, max, null_pct_max, unique, dtype, values

LOADER_RULES = {
    'ID': {'null_pct_max': 0, 'unique': True},
    'Severity': {'min': 1, 'max': 4, 'null_pct_max': 0},
    'Start_Time': {'null_pct_max': 0.01},
    'End_Time': {'null_pct_max': 0.01},
    'State': {'null_pct_max': 0.01},
}

CLEANER_RULES = {
    'ID': {'null_pct_max': 0, 'unique': True},
    'Severity': {'min': 1, 'max': 4, 'null_pct_max': 0},
    'Duration_min': {'min': 0, 'max': 1440, 'null_pct_max': 0.05},  # Tối đa 24h
    'Temperature(F)': {'min': -20, 'max': 120},
    'Visibility(mi)': {'min': 0, 'max': 10},
    'Year': {'min': 2019, 'max': 2022},
}

SPLITTER_RULES = {
    'fact': {
        'ID': {'null_pct_max': 0, 'unique': True},
        'Location_id': {'null_pct_max': 0},
        'weather_id': {'null_pct_max': 0},
        'full_date': {'null_pct_max': 0},
        'Severity': {'min': 1, 'max': 4},
        'Duration_min': {'min': 0, 'max': 1440},  # QUAN TRỌNG: Phải được cắt!
    },
    'dim_time': {
        'Date': {'null_pct_max': 0, 'unique': True},
        'Year': {'min': 2019, 'max': 2022},
    },
    'dim_location': {
        'Location_id': {'null_pct_max': 0, 'unique': True},
        'State': {'null_pct_max': 0},
    },
    'dim_weather': {
        'weather_id': {'null_pct_max': 0, 'unique': True},
    }
}

AGGREGATE_RULES = {
    'agg_federal': {
        'Year': {'min': 2019, 'max': 2022},
        'total_accidents': {'min': 1},
        'avg_severity': {'min': 1, 'max': 4},
        'high_severity_rate': {'min': 0, 'max': 100},
    },
    'agg_state': {
        'total_accidents': {'min': 1},
        'avg_severity': {'min': 1, 'max': 4},
        'pct_of_national': {'min': 0, 'max': 100},
        'severity_zscore': {'min': -10, 'max': 10},
    },
    'agg_weather': {
        'total_accidents': {'min': 1},
        'avg_severity': {'min': 1, 'max': 4},
        'severity_increase_pct': {'min': -50, 'max': 100},  # Hầu hết phải dương
        'duration_increase_pct': {'min': -50, 'max': 200},  # Đã cắt!
        'weather_risk_score': {'min': 0, 'max': 10},
    },
    'agg_city': {
        'total_accidents': {'min': 1},
        'pct_of_state': {'min': 0, 'max': 100},
        'cumulative_pct': {'min': 0, 'max': 100},
    }
}


# =============================================================================
# CÁC HÀM VALIDATION
# =============================================================================

@dataclass
class ValidationResult:
    """Kết quả của một lần kiểm tra."""
    passed: bool        # Đạt hay không
    stage: str          # Tên bước pipeline
    column: str         # Tên cột
    check: str          # Loại kiểm tra
    expected: Any       # Giá trị mong đợi
    actual: Any         # Giá trị thực tế
    message: str        # Thông báo


class ValidationError(Exception):
    """Exception khi validation thất bại."""
    def __init__(self, results: List[ValidationResult]):
        self.results = results
        failed = [r for r in results if not r.passed]
        messages = [f"  - {r.column}: {r.message}" for r in failed]
        super().__init__(f"Validation thất bại:\n" + "\n".join(messages))


def validate_column(df: pd.DataFrame, 
                    column: str, 
                    rules: Dict[str, Any],
                    stage: str = 'unknown') -> List[ValidationResult]:
    """
    Validate một cột theo các quy tắc.
    
    Tham số:
        df: DataFrame cần validate
        column: Tên cột
        rules: Dict {loại_check: giá_trị_mong_đợi}
        stage: Tên bước pipeline
        
    Trả về:
        Danh sách ValidationResult
    """
    results = []
    
    # Kiểm tra cột có tồn tại không
    if column not in df.columns:
        results.append(ValidationResult(
            passed=False,
            stage=stage,
            column=column,
            check='exists',
            expected='có mặt',
            actual='không tìm thấy',
            message=f"Không tìm thấy cột trong DataFrame"
        ))
        return results
    
    col_data = df[column]
    
    # Kiểm tra tỷ lệ null
    if 'null_pct_max' in rules:
        null_pct = col_data.isna().mean()
        max_allowed = rules['null_pct_max']
        passed = null_pct <= max_allowed
        results.append(ValidationResult(
            passed=passed,
            stage=stage,
            column=column,
            check='null_pct',
            expected=f"<= {max_allowed:.1%}",
            actual=f"{null_pct:.1%}",
            message=f"Tỷ lệ null {null_pct:.1%} > {max_allowed:.1%}" if not passed else "OK"
        ))
    
    # Kiểm tra giá trị tối thiểu
    if 'min' in rules:
        actual_min = col_data.min()
        expected_min = rules['min']
        passed = actual_min >= expected_min
        results.append(ValidationResult(
            passed=passed,
            stage=stage,
            column=column,
            check='min',
            expected=f">= {expected_min}",
            actual=actual_min,
            message=f"Min {actual_min} < {expected_min}" if not passed else "OK"
        ))
    
    # Kiểm tra giá trị tối đa
    if 'max' in rules:
        actual_max = col_data.max()
        expected_max = rules['max']
        passed = actual_max <= expected_max
        results.append(ValidationResult(
            passed=passed,
            stage=stage,
            column=column,
            check='max',
            expected=f"<= {expected_max}",
            actual=actual_max,
            message=f"Max {actual_max} > {expected_max}" if not passed else "OK"
        ))
    
    # Kiểm tra tính duy nhất
    if 'unique' in rules and rules['unique']:
        n_unique = col_data.nunique()
        n_total = len(col_data.dropna())
        passed = n_unique == n_total
        results.append(ValidationResult(
            passed=passed,
            stage=stage,
            column=column,
            check='unique',
            expected='tất cả duy nhất',
            actual=f"{n_unique}/{n_total}",
            message=f"Không duy nhất: {n_total - n_unique} bản ghi trùng" if not passed else "OK"
        ))
    
    # Kiểm tra giá trị cho phép
    if 'values' in rules:
        allowed = set(rules['values'])
        actual = set(col_data.dropna().unique())
        invalid = actual - allowed
        passed = len(invalid) == 0
        results.append(ValidationResult(
            passed=passed,
            stage=stage,
            column=column,
            check='values',
            expected=str(allowed),
            actual=str(actual),
            message=f"Giá trị không hợp lệ: {invalid}" if not passed else "OK"
        ))
    
    return results


def validate_stage(df: pd.DataFrame,
                   stage: str,
                   rules: Dict[str, Dict[str, Any]],
                   raise_on_fail: bool = True) -> List[ValidationResult]:
    """
    Validate DataFrame tại một bước pipeline.
    
    Tham số:
        df: DataFrame cần validate
        stage: Tên bước pipeline
        rules: Dict {cột: {check: giá_trị}}
        raise_on_fail: Nếu True, raise error khi thất bại
        
    Trả về:
        Danh sách tất cả ValidationResult
        
    Ví dụ:
        validate_stage(df, 'cleaner', CLEANER_RULES)
    """
    all_results = []
    
    print(f"\n  [{stage}] Đang validate {len(df):,} bản ghi, {len(rules)} cột...")
    
    for column, col_rules in rules.items():
        results = validate_column(df, column, col_rules, stage)
        all_results.extend(results)
    
    # Tổng kết
    passed = [r for r in all_results if r.passed]
    failed = [r for r in all_results if not r.passed]
    
    if failed:
        print(f"  [{stage}] THẤT BẠI: {len(failed)} check không đạt")
        for r in failed:
            print(f"    - {r.column}: {r.message}")
        
        if raise_on_fail:
            raise ValidationError(failed)
    else:
        print(f"  [{stage}] THÀNH CÔNG: {len(passed)} check OK")
    
    return all_results


def validate_referential_integrity(fact_df: pd.DataFrame,
                                   dim_df: pd.DataFrame,
                                   fact_fk: str,
                                   dim_pk: str,
                                   relationship_name: str = '') -> ValidationResult:
    """
    Validate quan hệ khóa ngoại giữa fact và dimension.
    
    Tham số:
        fact_df: Bảng fact
        dim_df: Bảng dimension
        fact_fk: Cột khóa ngoại trong fact
        dim_pk: Cột khóa chính trong dimension
        relationship_name: Tên quan hệ để hiển thị
        
    Trả về:
        ValidationResult
    """
    fact_values = set(fact_df[fact_fk].dropna().unique())
    dim_values = set(dim_df[dim_pk].dropna().unique())
    
    # Tìm các giá trị "mồ côi" (có trong fact nhưng không có trong dim)
    orphans = fact_values - dim_values
    
    passed = len(orphans) == 0
    
    return ValidationResult(
        passed=passed,
        stage='schema',
        column=f"{fact_fk} -> {dim_pk}",
        check='referential_integrity',
        expected='0 orphans',
        actual=f"{len(orphans)} orphans",
        message=f"{relationship_name}: {len(orphans)} bản ghi mồ côi" if not passed else "OK"
    )


# =============================================================================
# KIỂM TRA NHANH
# =============================================================================

def quick_sanity_check(df: pd.DataFrame, stage: str = '') -> None:
    """
    In kiểm tra nhanh cho bất kỳ DataFrame nào.
    Dùng ở CUỐI mỗi bước pipeline.
    
    Tham số:
        df: DataFrame cần kiểm tra
        stage: Tên bước
    """
    print(f"\n  [{stage}] Kiểm tra nhanh:")
    print(f"    Số bản ghi: {len(df):,}")
    print(f"    Số cột: {len(df.columns)}")
    
    # Kiểm tra các cột hay có vấn đề
    problem_cols = ['Duration_min', 'Severity', 'severity_impact_pct', 
                    'duration_impact_pct', 'pct_of_national', 'pct_of_state']
    
    for col in problem_cols:
        if col in df.columns:
            min_val = df[col].min()
            max_val = df[col].max()
            null_pct = df[col].isna().mean() * 100
            print(f"    {col}: [{min_val:.2f}, {max_val:.2f}], null={null_pct:.1f}%")


def assert_no_extreme_values(df: pd.DataFrame,
                             column: str,
                             min_val: Optional[float] = None,
                             max_val: Optional[float] = None,
                             stage: str = '') -> None:
    """
    Khẳng định cột không có giá trị cực đoan. Dừng ngay nếu có.
    
    Tham số:
        df: DataFrame
        column: Cột cần kiểm tra
        min_val: Giá trị tối thiểu cho phép
        max_val: Giá trị tối đa cho phép
        stage: Tên bước
        
    Raises:
        AssertionError nếu tìm thấy giá trị cực đoan
    """
    if column not in df.columns:
        return
    
    actual_min = df[column].min()
    actual_max = df[column].max()
    
    if min_val is not None:
        assert actual_min >= min_val, \
            f"[{stage}] {column} min={actual_min} < {min_val}"
    
    if max_val is not None:
        assert actual_max <= max_val, \
            f"[{stage}] {column} max={actual_max} > {max_val}"


def assert_percentages_valid(df: pd.DataFrame,
                             column: str,
                             should_sum_to_100: bool = False,
                             group_col: Optional[str] = None,
                             stage: str = '') -> None:
    """
    Khẳng định cột phần trăm hợp lệ (0-100).
    
    Tham số:
        df: DataFrame
        column: Cột phần trăm
        should_sum_to_100: Nếu True, kiểm tra tổng ~ 100
        group_col: Nếu có, kiểm tra tổng trong mỗi nhóm
        stage: Tên bước
    """
    if column not in df.columns:
        return
    
    assert df[column].min() >= 0, f"[{stage}] {column} có giá trị âm"
    assert df[column].max() <= 100, f"[{stage}] {column} > 100%"
    
    if should_sum_to_100:
        if group_col and group_col in df.columns:
            sums = df.groupby(group_col)[column].sum()
            bad_groups = sums[(sums < 99) | (sums > 101)]
            assert len(bad_groups) == 0, \
                f"[{stage}] {column} không tổng bằng 100% ở {len(bad_groups)} nhóm"
        else:
            total = df[column].sum()
            assert 99 <= total <= 101, \
                f"[{stage}] {column} tổng = {total}, mong đợi ~100"


# =============================================================================
# TEST MODULE
# =============================================================================

if __name__ == "__main__":
    print("Module Validators - Kiểm Tra Chất Lượng Dữ Liệu")
    print("=" * 50)
    
    # Test với data mẫu
    test_df = pd.DataFrame({
        'ID': ['A1', 'A2', 'A3'],
        'Severity': [1, 2, 5],      # 5 không hợp lệ!
        'Duration_min': [30, 2000, -10],  # 2000 và -10 không hợp lệ!
        'pct': [40, 35, 25]
    })
    
    print("\nData test:")
    print(test_df)
    
    print("\nChạy validation...")
    try:
        validate_stage(test_df, 'test', {
            'Severity': {'min': 1, 'max': 4},
            'Duration_min': {'min': 0, 'max': 1440}
        }, raise_on_fail=False)
    except ValidationError as e:
        print(f"\nLỗi Validation: {e}")
    
    print("\nKiểm tra nhanh:")
    quick_sanity_check(test_df, 'test')