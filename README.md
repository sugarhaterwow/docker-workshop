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
