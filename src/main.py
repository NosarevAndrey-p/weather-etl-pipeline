import requests
import datetime
from pathlib import Path
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, inspect



def extract(today_date, force_download=False):
    output_file = Path(f"data/raw/{today_date}/response.json")
    if not force_download and output_file.exists():
        print(f"Raw data already exists at {output_file}, skipping download.")
        return output_file

    # Compute start and end dates for API
    today_date_obj = datetime.datetime.strptime(today_date, "%Y-%m-%d").date()
    end_date_obj = today_date_obj - datetime.timedelta(days=1)
    start_date_obj = end_date_obj - datetime.timedelta(days=13)  # 14 days total
    start_date = start_date_obj.strftime("%Y-%m-%d")
    end_date = end_date_obj.strftime("%Y-%m-%d")

    payload = {
    "latitude":50.4547, 
    "longitude":30.5238, 
    "hourly":"temperature_2m,apparent_temperature,relative_humidity_2m,precipitation_probability,is_day,pressure_msl", 
    "timezone":"auto", 
    "start_date": start_date,
    "end_date": end_date
    }
    url = "https://api.open-meteo.com/v1/forecast"

    response = requests.get(url, params=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.json()}")

    output_file.parent.mkdir(exist_ok=True, parents=True)
    with open(output_file, 'w') as f:
        json.dump(response.json(), f)

    return output_file
    

def transform(output_file, today_date):
    with open(output_file, 'r') as f:
            raw = json.load(f)

    df = pd.DataFrame(raw['hourly'])
    df['time'] = pd.to_datetime(df['time'], format="%Y-%m-%dT%H:%M")
    tz = raw.get('timezone', None)
    if tz:
        df['time'] = df['time'].dt.tz_localize(tz).dt.tz_convert("UTC")

    df['precipitation_probability'] = df['precipitation_probability'] / 100.0 
    df['relative_humidity_2m'] = df['relative_humidity_2m'] / 100.0 
    df['is_day'] = df['is_day'].astype(bool)


    # Added a calculated field: "pressure_at_location"
    # Using the barometric formula to adjust pressure from mean sea level to the station elevation
    # P = P0 * exp(-M*g*h/(R*T))
    M = 0.0289644
    g = 9.80665
    R = 8.3144598
    h = raw['elevation']
    T = df['temperature_2m'] + 273.15  # to Kelvin
    df['pressure_at_location'] = df['pressure_msl'] * np.exp((-M * g * h) / (R * T))
    df = df.drop(columns=['pressure_msl'])

    # Save cleaned data as parquet
    output_cleaned_file = Path(f"data/processed/{today_date}/response.parquet")
    output_cleaned_file.parent.mkdir(exist_ok=True, parents=True)
    df.to_parquet(output_cleaned_file, engine='pyarrow', index=False)

    return output_cleaned_file

def create_db(output_cleaned_file, force_recreate=False):
    df = pd.read_parquet(output_cleaned_file, engine='pyarrow')

    db_path = Path("data/local.db")
    db_path.parent.mkdir(exist_ok=True, parents=True)
    engine = create_engine(f"sqlite:///{db_path}")

    if force_recreate:
        df.to_sql("weather_data", con=engine, if_exists="replace", index=False)
    else:
        inspector = inspect(engine)
        if inspector.has_table("weather_data"):
            existing_times = pd.read_sql("SELECT time FROM weather_data", con=engine)['time']

            if not existing_times.empty:
                existing_times_set = set(pd.to_datetime(existing_times).dt.tz_localize("UTC"))
                df = df[~df['time'].isin(existing_times_set)]
        if not df.empty:
            df.to_sql("weather_data", con=engine, if_exists="append", index=False)

    return db_path

def analytics(db_path, today_date):
    report = {}

    engine = create_engine(f"sqlite:///{db_path}")

    with engine.connect() as conn:
        # Average temperature over the last 7 days
        result = conn.execute(text("""
            SELECT AVG(temperature_2m) as avg_temp_7d
            FROM weather_data
            WHERE time >= datetime('now', '-7 days', 'utc') AND time < datetime('now', 'utc')
        """))
        report['avg_temp_last7d'] = result.fetchone()[0]

        # Average temperature grouped by day/night over the last 7 days
        result = conn.execute(text("""
            SELECT AVG(temperature_2m) as avg_temp_7d, is_day
            FROM weather_data
            WHERE time >= datetime('now', '-7 days', 'utc') AND time < datetime('now', 'utc')
            GROUP BY is_day
        """))
        avg_by_daynight = {}
        for avg_temp, is_day in result.fetchall():
            avg_by_daynight['Day' if is_day==1 else 'Night'] = avg_temp
        report['avg_temp_by_daynight'] = avg_by_daynight

        # Count of days with average pressure below 1000 mmHg
        result = conn.execute(text("""
            SELECT COUNT(*) AS days_below_1000mmHg
            FROM (
                SELECT 
                    DATE(datetime(time, '+3 hours')) AS local_day,
                    AVG(pressure_at_location) AS avg_pressure
                FROM weather_data
                GROUP BY local_day
            ) AS daily_avg
            WHERE avg_pressure < 1000
        """))
        report['days_below_1000mmHg'] = result.fetchone()[0]

        # Count of hours where apparent temperature > actual temperature in the last 7 days
        result = conn.execute(text("""
            SELECT COUNT(*) AS hours_apparent_gt_actual
            FROM weather_data
            WHERE apparent_temperature > temperature_2m
            AND time >= datetime('now', '-7 days', 'utc') AND time < datetime('now', 'utc')
        """))
        report['hours_temp_apparent_greater_actual'] = result.fetchone()[0]

    # Save report as JSON
    output_report_file = Path(f"data/reports/{today_date}/report.json")
    output_report_file.parent.mkdir(exist_ok=True, parents=True)

    with open(output_report_file, 'w') as f:
        json.dump(report, f, indent=4, default=float)
    
    return output_report_file

def etl_pipeline(
    run_date: str = "", 
    force_download: bool = True, 
    force_recreate: bool = True
):
    """
    ETL pipeline to fetch weather data, transform, store in SQLite, and generate report.

    Parameters:
        run_date (str): Optional date in "YYYY-MM-DD" format. Defaults to today.
        force_download (bool): If True, forces re-downloading raw data.
        force_recreate (bool): If True, forces recreating the database table.
    """
    print("Starting ETL pipeline...")


    today_date_obj = datetime.date.today()
    if run_date == "":
        today_date_obj = datetime.date.today()
    else:
        try:
            today_date_obj = datetime.datetime.strptime(run_date, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError(f"Invalid date format: '{run_date}'. Use YYYY-MM-DD.")
        if today_date_obj > datetime.date.today():
            raise ValueError(f"Run date '{run_date}' cannot be in the future.")
    today_date = today_date_obj.strftime("%Y-%m-%d")

    print("Extracting raw data...")
    output_file = extract(today_date, force_download=force_download)
    print(f"Raw data saved to: {output_file}")

    print("Transforming data...")
    output_cleaned_file = transform(output_file, today_date)
    print(f"Cleaned data saved to: {output_cleaned_file}")

    print("Creating/updating database...")
    db_path = create_db(output_cleaned_file, force_recreate=force_recreate)
    print(f"Database ready at: {db_path}")

    print("Running analytics...")
    output_report_file = analytics(db_path, today_date)
    print(f"Report generated at: {output_report_file}")

    print("ETL pipeline completed successfully.\n")


if __name__ == "__main__":
    etl_pipeline()