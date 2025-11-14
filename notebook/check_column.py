import pandas as pd

# Load raw data (just first few rows for speed)
df = pd.read_csv('data/raw/US_Accidents_March23.csv', nrows=1000)

print("\n" + "="*60)
print("COLUMNS IN RAW DATA")
print("="*60)
print(f"\nTotal columns: {len(df.columns)}\n")

for i, col in enumerate(df.columns, 1):
    dtype = df[col].dtype
    non_null = df[col].notna().sum()
    sample = df[col].dropna().iloc[0] if non_null > 0 else "N/A"
    
    print(f"{i:2d}. {col:<30} | {str(dtype):<15} | Sample: {sample}")

print("\n" + "="*60)

# Check for lat/lng columns
print("\nLocation-related columns:")
location_cols = [col for col in df.columns if any(x in col.lower() for x in ['lat', 'lng', 'lon', 'coord'])]
for col in location_cols:
    print(f"  - {col}")

print("\n" + "="*60)