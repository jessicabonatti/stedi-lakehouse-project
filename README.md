# STEDI Human Balance - Data Lakehouse Project

This project was developed as part of the **Udacity Data Engineering Nanodegree - Data Lakes with Spark and AWS Glue**.

The goal was to build a Lakehouse architecture using AWS Glue, S3, Athena, and Apache Spark to process sensor data from the STEDI human balance trainer device. 
This includes anonymizing sensitive data and preparing curated datasets for machine learning.

---

## Project Structure

project-root/  
│  
├── glue_jobs/  
│   ├── customer_landing_to_trusted.py  
│   ├── accelerometer_landing_to_trusted.py  
│   └── step_trainer_trusted.py  
│  
├── sql/
│   ├── customer_landing.sql  
│   ├── accelerometer_landing.sql  
│   └── step_trainer_landing.sql  
│  
├── screenshots/  
│   ├── customer_landing.png  
│   ├── accelerometer_trusted.png  
│   ├── accelerometer_landing.png  
│   ├── accelerometer_trusted.png  
│   ├── step_trainer_landing.png   
│   ├── trainer_trusted.png   
│   ├── machine_learning_curated.png  
│  
└── README.md  

---

## Data Sources

- `customer/landing`: Customer registration data
- `accelerometer/landing`: Mobile app accelerometer data
- `step_trainer/landing`: Sensor readings from the STEDI Step Trainer device

---

## AWS Glue Jobs

| Glue Job Name                  | Description                                                             |
|-------------------------------|-------------------------------------------------------------------------|
| `customer_landing_to_trusted` | Filters only customers who consented to share their data                |
| `accelerometer_landing_to_trusted` | Joins accelerometer data with trusted customers and removes PII       |
| `step_trainer_trusted`        | Filters Step Trainer sensor data to only include trusted customers      |
| `machine_learning_curated`    | Joins step trainer and accelerometer data for ML model training         |

---

## Athena Queries

Sample queries used to validate data:

```sql
-- Check number of trusted customers
SELECT COUNT(*) FROM stedi.customer_trusted;

-- Validate final ML curated dataset
SELECT COUNT(*) FROM stedi.machine_learning_curated;
```

---

## Tables Rows count
•	customer_landing - 956  
•	accelerometer_landing - 81273  
•	step_trainer_landing - 28680  
•	customer_trusted - 482  
•	accelerometer_trusted - 40981  
•	step_trainer_trusted - 14460  
•	customer_curated - 482  
•	machine_learning_curated - 43681  

---

## Notes
	•	All Glue jobs are saved in the glue_jobs/ folder.  
	•	SQL scripts for table creation are in the sql/ folder.  
	•	Query result screenshots are stored in the screenshots/ folder.  
	•	The final datasets are stored in Amazon S3 and are queryable via Athena.  

⸻

## Author

Jessica Bonatti
