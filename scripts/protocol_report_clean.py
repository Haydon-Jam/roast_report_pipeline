import pandas as pd
import xlrd
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from pathlib import Path

#---------------------------------------------------------------------------------------------
#Dynamic user input converted to Path for output naming functionality
raw_input_str = input("Enter the path to the production report Excel file: ")
input_path = Path(raw_input_str)

df = pd.read_excel(input_path)

#Prepping additional CSV output path
out_dir = Path(__file__).parent.parent / "data" / "processed"
out_dir.mkdir(parents=True, exist_ok=True) 

#Build your processed filename off of .stem
processed_name = f"{input_path.stem}.processed.csv"
output_path   = out_dir / processed_name

#---------------------------------------------------------------------------------------------
#Alias for the logging variable later
file_path = input_path


#---------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------
#DATA CLEAN AND FORMATTING

#Remove the total row the export adds at the bottom
df.dropna(inplace=True)

#Extract date and time separately and drop original date column
#Avoid SQL reserved words 'date', 'time' ect
df['r_date'] = pd.to_datetime(df['Date']).dt.date
df['r_time'] = pd.to_datetime(df['Date']).dt.time
df = df.drop(columns={'Date'})

#Improve column header format
df.rename(columns={
    'ID-Tag': 'id_tag',
    'Start Weight': 'green_vol',
    'End Weight': 'end_weight',
    'Weight Loss': 'weight_loss',
    'Components': 'coffee_inv',
    'Profile': 'r_profile'
}, inplace=True)

df['week_number'] = pd.to_datetime(df['r_date']).dt.isocalendar().week
df['day_name'] = pd.to_datetime(df['r_date']).dt.day_name()

df['week_start'] = pd.to_datetime(df['r_date']).apply(lambda d: d - pd.Timedelta(days=d.weekday()))

df['week_start'] = pd.to_datetime(df['week_start']).dt.date

#Enable future machine specific DB queries
df['machine'] = df.apply(lambda x: 'G75' if x['end_weight'] > 10.0 else 'UG-15', axis=1)

#Isolate PG-Code for use as potential SQL Primary Key
df['pg_code'] = df['coffee_inv'].str.extract(r'(PG-\d+)', expand=False)


df['r_date'] = pd.to_datetime(df['r_date'])


#Specify a custom Financial year - In this case August 
def get_fiscal_year_start(date):
    """Return the start (Monday) of the fiscal year for a given date."""
    year = date.year
    first_august = pd.Timestamp(year=year, month=8, day=1)

    # Find the first Sunday in August
    first_sunday = first_august + pd.DateOffset(days=(6 - first_august.weekday()))  # Sunday = 6

    fiscal_start = first_sunday - pd.DateOffset(days=6)  # Backtrack to the Monday of that week

    if date < fiscal_start:
        # Belongs to the previous fiscal year
        year -= 1
        first_august = pd.Timestamp(year=year, month=8, day=1)
        first_sunday = first_august + pd.DateOffset(days=(6 - first_august.weekday()))
        fiscal_start = first_sunday - pd.DateOffset(days=6)

    return fiscal_start

def get_fiscal_week(date):
    date = pd.Timestamp(date)
    fy_start = get_fiscal_year_start(date)
    return ((date - fy_start).days // 7) + 1

def get_fiscal_year_label(date):
    fy_start = get_fiscal_year_start(date)
    fy_end_year = fy_start.year + 1
    return f"{fy_start.year}-{fy_end_year}"

# Apply to DataFrame
df['financial_week'] = df['r_date'].apply(get_fiscal_week)
df['financial_year'] = df['r_date'].apply(get_fiscal_year_label)

df['r_date'] = pd.to_datetime(df['r_date']).dt.date

#column order
desired_order = ['week_start','financial_year','financial_week','week_number','r_date','day_name','r_time','id_tag', 'r_profile', 'coffee_inv', 'pg_code', 'green_vol', 'end_weight', 'machine']
df = df[desired_order]

df['week_start'] = pd.to_datetime(df['week_start']).dt.date
df['r_date'] = pd.to_datetime(df['r_date'])         # full datetime if available
df['r_time'] = pd.to_datetime(df['r_time'], format='%H:%M:%S', errors='coerce').dt.time  # optional if separate time

df['week_start'] = pd.to_datetime(df['week_start'], errors='coerce').dt.date

df['financial_year'] = df['financial_year'].astype(str).str.strip()

#---------------------------------------------------------------------------------------------
#Send a copy of the processed data to CSV

df.to_csv(output_path, index=False)
print(f"Wrote processed CSV to {output_path}")

#---------------------------------------------------------------------------------------------
#SQL DB Appending using SQLAlchemy create_engine

# Point directly to the docker folder .env file
dotenv_path = Path(__file__).resolve().parent.parent / 'docker' / '.env'
load_dotenv(dotenv_path=dotenv_path)


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("POSTGRES_DB")

print("Env vars loaded:")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
)

# Append the DataFrame to the table
df.to_sql(
    name="protocol_reports",
    con=engine,
    if_exists="append",
    index=False
)

#---------------------------------------------------------------------------------------------
#LOGGING

from datetime import datetime
import csv

log_path = os.path.join("data", "log", "log.csv")
file_name = os.path.basename(file_path)
timestamp = datetime.now().isoformat(timespec='seconds')
row_count = len(df)

log_entry = [file_name, row_count, timestamp, "Success"]

# Ensure the folder exists
os.makedirs(os.path.dirname(log_path), exist_ok=True)

# Write header if file doesn't exist
write_header = not os.path.exists(log_path)

with open(log_path, mode='a', newline='') as log_file:
    writer = csv.writer(log_file)
    if write_header:
        writer.writerow(["filename", "rows_uploaded", "timestamp", "status"])
    writer.writerow(log_entry)

print(f"📝 Log updated at {log_path}")