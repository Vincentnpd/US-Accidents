import pandas as pd
import os

class DataLoader:
    def __init__(self, source="csv"):
        self.source = source

    def load_filtered_csv(
        self, path, start_year=2019, end_year=2023,
        date_col="Start_Time", usecols=None, chunksize=500_000
    ):
        if not os.path.exists(path):
            print(f"File không tồn tại: {path}")
            return pd.DataFrame()

        print(f"Đang load dữ liệu từ {start_year} đến {end_year}...")
        filtered_chunks = []

        for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize):
            if date_col not in chunk.columns:
                print(f"Không tìm thấy cột '{date_col}'. Các cột có: {chunk.columns.tolist()}")
                return pd.DataFrame()

            chunk[date_col] = pd.to_datetime(chunk[date_col], errors="coerce")
            mask = (chunk[date_col].dt.year >= start_year) & (chunk[date_col].dt.year <= end_year)
            filtered = chunk.loc[mask]

            if not filtered.empty:
                filtered_chunks.append(filtered)

        if not filtered_chunks:
            print("Không có dữ liệu trong khoảng năm được chọn.")
            return pd.DataFrame()

        df_filtered = pd.concat(filtered_chunks, ignore_index=True)
        print(f"Đã load {len(df_filtered):,} dòng ({start_year}-{end_year})")
        return df_filtered
