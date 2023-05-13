CREATE EXTERNAL TABLE IF NOT EXISTS nycTaxi (
  vendorid STRING,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  ratecode INT,
  passenger_count INT,
  trip_distance DOUBLE,
  fare_amount DOUBLE,
  total_amount DOUBLE,
  payment_type INT
)
PARTITIONED BY (year STRING, month STRING, type STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format'='1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://serverless-analytics/canonical/NY-Pub';

MSCK REPAIR TABLE `nycTaxi`;
