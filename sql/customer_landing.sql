CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  serialNumber string,
  shareWithPublicAsOfDate string,
  birthDay string,
  registrationDate string,
  shareWithResearchAsOfDate string,
  customerName string,
  email string,
  lastUpdateDate string,
  phone string,
  shareWithFriendsAsOfDate string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://jb-bucket-2025/customer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
