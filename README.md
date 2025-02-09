# ETL_Toxigen
This ETL pipeline processes the TOXIGEN dataset, performing the following steps:

- Extract: Load data from a CSV file.

- Transform: Clean and preprocess text, categorize toxicity levels, and optimize data types.

- Load: Insert transformed data into a PostgreSQL database.

This pipeline is optimized for efficient memory usage and structured data storage.

## Dependencies
Ensure Python 3.8+ is installed, along with the following libraries:
- Pandas
- SQLAlchemy
- psutil
- psycopg2

`pip install pandas sqlalchemy psycopg2 psutil`

PostgreSQL version 17 is used for this project. Install PostgreSQL and use default settings along with setting your own database password. For this project's purposes, the password used is **thepassword**.

## Database Setup & Execution Instructions
After PostgreSQL is installed and running, open the **psql** command terminal by logging in using the credentials set. Then, run the following commands: 
1. Create a new database:
```
CREATE DATABASE toxigen_db;
```
2. Connect to the newly formed database using `\c toxigen_db`.

3. Within `toxigen_db`, create the table by running this command line:
```
CREATE TABLE toxigen_data (
    id SERIAL PRIMARY KEY,
    prompt TEXT NOT NULL,
    generation TEXT NOT NULL,
    generation_method VARCHAR(10),
    "group" VARCHAR(255),
    prompt_label VARCHAR(10),
    roberta_prediction DECIMAL(5,4),
    toxicity_level VARCHAR(10)
);
```

### Running the ETL Pipeline
Run the Python script **etl_pipeline.py**:
```
python etl_pipeline.py
```


