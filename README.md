# week1_homework
## Question 1
docker run --rm python:3.13 pip --version
## Question 2
None
## Question 3
docker run -it --rm -v $(pwd):/data duckdb/duckdb:latest

SELECT COUNT(*) AS num_trips
FROM trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
## Question 4
WITH daily_max AS (
    SELECT DATE(lpep_pickup_datetime) AS pickup_date,
           MAX(trip_distance) AS max_distance
    FROM '/data/green_tripdata_2025-11.parquet'
    WHERE trip_distance < 100
    GROUP BY DATE(lpep_pickup_datetime)
)
SELECT pickup_date
FROM daily_max
ORDER BY max_distance DESC
LIMIT 1;

## Question 5
SELECT z.Zone, SUM(t.total_amount) AS total
FROM '/data/green_tripdata_2025-11.parquet' t
JOIN '/data/taxi_zone_lookup.csv' z
  ON t.PULocationID = z.LocationID
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z.Zone
ORDER BY total DESC
LIMIT 1;

## Question 6
-- 1️⃣ 先创建 zones 表
CREATE OR REPLACE TABLE taxi_zones AS
SELECT
    CAST(LocationID AS INTEGER) AS LocationID,
    Borough,
    Zone,
    service_zone
FROM read_csv_auto('/data/taxi_zone_lookup.csv');

-- 2️⃣ 直接查询：East Harlem North 出发，哪儿下车小费总额最大
SELECT z_drop.Zone AS dropoff_zone,
       SUM(t.tip_amount) AS total_tip
FROM '/data/green_tripdata_2025-11.parquet' t
JOIN taxi_zones z_pick
  ON t.PULocationID = z_pick.LocationID
JOIN taxi_zones z_drop
  ON t.DOLocationID = z_drop.LocationID
WHERE z_pick.Zone = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY z_drop.Zone
ORDER BY total_tip DESC
LIMIT 1;

## Question 7
cd week_1_homework
wget https://releases.hashicorp.com/terraform/1.6.3/terraform_1.6.3_linux_amd64.zip
unzip terraform_1.6.3_linux_amd64.zip
sudo mv terraform /usr/local/bin/
terraform -version

export GOOGLE_APPLICATION_CREDENTIALS="<PATH_TO_PROJECT>.json"
gcloud auth application-default login
gcloud auth application-default print-access-token

terraform init
terraform plan
terraform apply -auto-approve

# week2_homework
## Question 1
import requests

url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/yellow_tripdata_2020-12.csv"
local_filename = "yellow_tripdata_2020-12.csv"

response = requests.get(url, stream=True)

with open(local_filename, 'wb') as f:
    for chunk in response.iter_content(chunk_size=1024):
        if chunk:
            f.write(chunk)

print(f"File downloaded and saved as {local_filename}")

## Question 2
import os
import pandas as pd


file_path = "yellow_tripdata_2020-12.csv"
data = pd.read_csv(file_path)

file_size = os.path.getsize(file_path) / (1024 * 1024)  # 转换为 MB
print(f"The uncompressed file size is: {file_size:.2f} MB")

## Question 3
import pandas as pd

# 假设的年份和月份组合
years = [2020]
months = [1, 2, 3, 4, 5, 6, 7]  
taxi_types = ['yellow', 'green']

for year in years:
    for month in months:
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv"
            print(f"Processing {filename}")
            
            # data = pd.read_csv(filename)


## Question 4
import schedule
import time
from datetime import datetime
import pytz

ny_timezone = pytz.timezone('America/New_York')

def job():
    print(f"Job executed at {datetime.now(ny_timezone)}")

schedule.every(1).minute.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

## Question 5
import pandas as pd

yellow_data = pd.read_csv("yellow_tripdata_2020.csv")

num_rows = len(yellow_data)
print(f"Number of rows in Yellow Taxi data for 2020: {num_rows}")


## Question 6
inputs = {
    'taxi': 'green',
    'year': 2020,
    'month': 4
}

file_name = f"{inputs['taxi']}_tripdata_{inputs['year']}-{inputs['month']:02d}.csv"
print(f"Rendered file name: {file_name}")


