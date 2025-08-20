CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
  timestamp string,
  user string,
  x float,
  y float,
  z float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://jb-bucket-2025/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
