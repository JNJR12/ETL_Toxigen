import pandas as pd
from sqlalchemy import create_engine

# db connection parameters
DB_NAME = "toxigen_db"
DB_USER = "postgres"
DB_PASSWORD = "thepassword"
DB_HOST = "localhost"
DB_PORT = "5432"

# create db conn
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

df = pd.read_csv("../data/toxigen_cleaned.csv")

df.to_sql("toxigen_data", engine, if_exists="append", index=False)

print("Data successfully loaded into PostgreSQL!")
