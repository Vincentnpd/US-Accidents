import pandas as pd

class DataLoader:
    """
    Lớp chịu trách nhiệm nạp dữ liệu từ nhiều nguồn (CSV, Excel, API)
    """
    def __init__(self, source_type="csv"):
        self.source_type = source_type

    def load_from_csv(self, file_path: str) -> pd.DataFrame:
        """Đọc dữ liệu từ file CSV"""
        try:
            df = pd.read_csv(file_path)
            print(f" Đã nạp dữ liệu từ CSV: {df.shape[0]} dòng, {df.shape[1]} cột")
            return df
        except FileNotFoundError:
            raise Exception(f" Không tìm thấy file: {file_path}")
        except Exception as e:
            raise Exception(f"Lỗi khi đọc CSV: {e}")

    def load_from_excel(self, file_path: str, sheet_name=None) -> pd.DataFrame:
        """Đọc dữ liệu từ Excel"""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            print(f" Đã nạp dữ liệu từ Excel: {df.shape[0]} dòng, {df.shape[1]} cột")
            return df
        except Exception as e:
            raise Exception(f"Lỗi khi đọc Excel: {e}")

    def load_from_api(self, url: str) -> pd.DataFrame:
        """(Tuỳ chọn) Đọc dữ liệu từ API"""
        import requests
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            print(f" Đã nạp dữ liệu từ API: {df.shape[0]} dòng")
            return df
        except Exception as e:
            raise Exception(f"Lỗi khi gọi API: {e}")
