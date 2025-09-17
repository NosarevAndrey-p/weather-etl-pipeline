# Weather ETL Pipeline

This project is a simple ETL pipeline that fetches weather data from an API, processes it, stores it in a SQLite database, and generates summary reports. It is implemented in Python and can be run as a standalone script.

---

## Quick start

Run setup.sh to create virtual environment and install dependencies.
```bash
source ./setup.sh
```
Then run pipeline itself
```bash
python src/main.py
```

## API Used

* **Open-Meteo API**
  Endpoint: `https://api.open-meteo.com/v1/forecast`
  Parameters used:

  * `latitude`: 50.4547 (Kyiv, Ukraine)
  * `longitude`: 30.5238
  * `hourly`: `temperature_2m,apparent_temperature,relative_humidity_2m,precipitation_probability,is_day,pressure_msl`
  * `timezone`: `auto`
  * `start_date`, `end_date`: calculated automatically (14 days window ending yesterday)

---

## Fields Selected and Processed

| Field                       | Description                             | Notes                                                   |
| --------------------------- | --------------------------------------- | ------------------------------------------------------- |
| `time`                      | Timestamp of the observation            | Converted to UTC                                        |
| `temperature_2m`            | Actual air temperature (°C)             | Direct from API                                         |
| `apparent_temperature`      | Feels-like temperature (°C)             | Direct from API                                         |
| `relative_humidity_2m`      | Relative humidity (0-1)                 | Converted from percentage                               |
| `precipitation_probability` | Probability of precipitation (0-1)      | Converted from percentage                               |
| `is_day`                    | Daylight flag                           | Converted to boolean                                    |
| `pressure_at_location`      | Pressure adjusted for station elevation | Calculated using barometric formula from `pressure_msl` |

The `pressure_msl` field from the API is dropped after calculating `pressure_at_location`.

---

## Analytics / Reports

The pipeline generates a JSON report with the following metrics:

* Average temperature over the last 7 days (`avg_temp_last7d`)
* Average temperature by day/night over the last 7 days (`avg_temp_by_daynight`)
* Count of days with average pressure below 1000 mmHg (`days_below_1000mmHg`)
* Count of hours where apparent temperature exceeds actual temperature over the last 7 days (`hours_temp_apparent_greater_actual`)

Reports are saved under `data/reports/YYYY-MM-DD/report.json`.

---

## Project Structure

```
.
├── data/                   # Raw, processed, and report data
├── src/
│   └── main.py             # ETL pipeline code
├── requirements.txt        # Python dependencies
└── setup.sh                # Setup virtual environment & install dependencies
```

---

## Instructions to Run

1. **Set up the environment:**

```bash
source ./setup.sh
```

2. **Run the ETL pipeline (default runs for today date):**

```bash
python src/main.py
```

3. **Optional command-line arguments:**

| Flag                            | Description                          |
| ------------------------------- | ------------------------------------ |
| `-d` or `--run_date YYYY-MM-DD` | Specify run date (defaults to today) |
| `-fd` or `--force_download`     | Force re-download of raw data, otherwise if data for specified date present on disk, just load them        |
| `-fr` or `--force_recreate`     | Force recreate database table, otherwise only new entries will be appended to database        |

Example:

```bash
python src/main.py -d 2025-09-15 -fd -fr
```

## Dependencies

* `requests`
* `pandas`
* `numpy`
* `sqlalchemy`
* `pyarrow` (for Parquet support)

---

## Notes

* The SQLite database is stored in `data/local.db`.
* Raw API responses are saved in `data/raw/YYYY-MM-DD/response.json`.
* Transformed data is saved in Parquet format under `data/processed/YYYY-MM-DD/response.parquet`.
* Report is saved in JSON format under `data/reports/YYYY-MM-DD/report.json`.
