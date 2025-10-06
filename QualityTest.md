# Kiểm tra chất lượng dữ liệu
## Mục tiêu: Kiểm tra chất lượng dữ liệu -> Tìm các vấn đề như:

> Thiếu dữ liệu.
Dữ liệu không hợp lệ hoặc không nhất quán.
Dữ liệu trùng lặp.
Các giá trị ngoại lệ (outliers).
Kiểu dữ liệu chưa phù hợp.
### 1. Đọc dữ liệu
```
import pandas as pd
df = pd.read_csv("US_Accidents_March23.csv")
```

### 2. Tổng quan dữ liệu
```
df.info()
print(df.describe())
print(df.head())
```
Mục đích
- Kiểm tra số dòng, số cột
- Kiểm tra kiểu dữ  (data types)
- Phát hiện các cột có nhiều giá trị thiếu (Null,NaN)
- In ra 5 hàng giá trị đầu tiên
### 3. Kiểm tra dữ liệu thiếu
```
missing_values=df.isnull().sum()
missing_percentage=(missing_values/len(df))*100

missing_df=pd.DataFrame({
    'Misssing Values'   : missing_values,
    'Percentage'        : missing_
}).sort_(by='Percentage', ascending= False)

print(missing_df.head(20)) # Xem top 20 cột có dữ liệu thiếu
```
### 4. Kiểm tra dữ liệu trùng lặp
```
duplicate_rows = df.duplicated().sum()
print(f'Số dòng trùng lặp: {duplicate_rows}')
```
Nếu có nhiều bản ghi trùng lặp nhau thì có thể xóa sử dụng câu lệnh:
> df = df.drop_duplicates()

### 5. Kiểm tra kiểu dữ liệu (Data Types)
Đảm bảo các cột có kiểu dữ liệu hợp lý:
```
df['Start_time'] = pd.to_datetime(df['Start_time'])
df['End_time'] = pd.to_datetime(df['End_time'])
```
### 6. Phát hiện các giá trị bất thường
Tìm các giá trị bất thường, nằm ngoài phạm vi cho phép.
```
numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns

print("Phát hiện outliers dựa trên IQR:")

for col in numeric_cols:
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
    print(f"{col}: {len(outliers)} giá trị ngoại lệ")
```
### 7. Kiểm tra tính nhất quán của dữ liệu
Kiểm tra các giá trị trong cột dạng danh mục có hợp lý, không có lỗi đánh máy hay giá trị lạ.
```
categorical_cols = ['Severity', 'State', 'Weather_Condition']  # bạn thay đổi theo dataset

for col in categorical_cols:
    if col in df.columns:
        print(f"\nGiá trị duy nhất của cột {col}:")
        print(df[col].value_counts(dropna=False).head(20))  # in top 20 giá trị
```
### 8. Tổng hợp và đề xuất xử lý
- Các cột có % dữ liệu thiếu cao -> Xem xét loại bỏ cột hoặc dùng phương pháp điền giá trị (imputation).

- Dòng trùng lặp -> loại bỏ df.drop_duplicates().

- Ngoại lệ -> kiểm tra lại hoặc loại bỏ tùy ngữ cảnh.

- Kiểu dữ liệu -> chuyển đổi đúng kiểu, đặc biệt cột ngày tháng.

- Categorical -> chuẩn hóa giá trị.