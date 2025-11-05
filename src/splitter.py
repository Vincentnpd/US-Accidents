import os
import pandas as pd

class DataSplitter:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def split_thoi_gian(self) -> pd.DataFrame:
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'], errors='coerce')
        self.df['End_Time'] = pd.to_datetime(self.df['End_Time'], errors='coerce')

        time_df = self.df[['Start_Time', 'End_Time']].drop_duplicates().reset_index(drop=True)
        time_df["Date"] = time_df["Start_Time"].dt.date
        time_df["Year"] = time_df["Start_Time"].dt.year
        time_df["Month"] = time_df["Start_Time"].dt.month
        time_df["Day"] = time_df["Start_Time"].dt.day
        time_df["Hour"] = time_df["Start_Time"].dt.hour

        # Duration dạng giờ:phút
        duration = (time_df["End_Time"] - time_df["Start_Time"])
        time_df["Duration"] = (
            duration.dt.components["hours"].astype(str)
            + ":" +
            duration.dt.components["minutes"].astype(str).str.zfill(2)
        )

        return time_df

    def split_vi_tri(self) -> pd.DataFrame:
        location_cols = ["Street", "City", "State", "Zipcode", "Country","Amenity", "Bump", "Crossing",
                         "Give_Way", "Junction","No_Exit", "Railway", "Roundabout", "Station","Stop",]
        location_df = self.df[location_cols].drop_duplicates(subset=["Street"]).reset_index(drop=True)
        location_df["Location_ID"] = range(1, len(location_df) + 1)
        return location_df

    def split_fact(self, location_df: pd.DataFrame) -> pd.DataFrame:
        df = self.df.merge(location_df[["Street", "Location_ID"]], on="Street", how="left")
        fact_cols = [
            "ID", "Severity","Start_Time", "End_Time", "Weather_Condition", "Visibility(mi)",
            "Temperature(F)", "Humidity(%)", "Pressure(in)", "Wind_Speed(mph)","Wind_Chill(F)","Wind_Direction",
            "Precipitation(in)", "Location_ID", "Description",
            "Number","Street","City","Timezone","Weather_Timestamp",
        ]
        fact_df = df[[col for col in fact_cols if col in df.columns]].drop_duplicates().reset_index(drop=True)
        return fact_df

    def export_all(self, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        time_df = self.split_thoi_gian()
        location_df = self.split_vi_tri()
        fact_df = self.split_fact(location_df)

        time_df.to_csv(os.path.join(output_dir, "thoi_gian.csv"), index=False)
        location_df.to_csv(os.path.join(output_dir, "vi_tri.csv"), index=False)
        fact_df.to_csv(os.path.join(output_dir, "Accident_Detail.csv"), index=False)
        print("Đã xuất tất cả bảng vào thư mục:", output_dir)
