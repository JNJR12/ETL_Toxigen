import pandas as pd
import re
from sqlalchemy import create_engine
import time
import psutil

# db conn parameters
DB_NAME = "toxigen_db"
DB_USER = "postgres"  
DB_PASSWORD = "thepassword"  
DB_HOST = "localhost"
DB_PORT = "5432"

start_db = time.time()
# postgresql connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
end_db = time.time()

start_time = time.time()
# extract data
start_extract = time.time()
file_path = "../data/toxigen.csv"
df = pd.read_csv(file_path)
end_extract = time.time()
print(f"Extraction Time: {end_extract - start_extract:.2f} seconds")



# transform data
start_transform = time.time()
def clean_prompt(text):
    if isinstance(text, str):
        text = text.strip()  # trim space
        text = text.replace("\\n", "\n")  # convert '\n' to newline
        text = re.sub(r"\n-\s*$", "", text)  # remove trailing hyphen
    return text

df["prompt"] = df["prompt"].apply(clean_prompt)

# remove rows with missing values in 'generation' column
df = df.dropna(subset=["generation"])
df = df[df["generation"].str.strip() != ""]

# normalize text fields
df["prompt"] = df["prompt"].str.strip().str.lower()
df["generation"] = df["generation"].str.strip().str.lower()
df["group"] = df["group"].str.strip()

# categorize toxicity level based on roberta prediction
def categorize_toxicity(prob):
    if prob >= 0.75:
        return "High"
    elif prob >= 0.5:
        return "Medium"
    elif prob >= 0.25:
        return "Low"
    else:
        return "Minimal"

df["toxicity_level"] = df["roberta_prediction"].apply(categorize_toxicity)

df["prompt_label"] = df["prompt_label"].map({0: "Benign", 1: "Toxic"})


df = df.drop_duplicates()

# improve memory usage
df["generation_method"] = df["generation_method"].astype("category")
df["group"] = df["group"].astype("category")
df["prompt_label"] = df["prompt_label"].astype("category")
df["toxicity_level"] = df["toxicity_level"].astype("category")

end_transform = time.time()
print(f"Transformation Time: {end_transform - start_transform:.2f} seconds")

# load data into PostgreSQL
start_load = time.time()
df.to_sql("toxigen_data", engine, if_exists="replace", index=False)
end_load = time.time()
print(f"Load Time: {end_load - start_load:.2f} seconds")

print("ETL Pipeline completed successfully! Data loaded into PostgreSQL.")
end_time = time.time()
print(f"Total ETL runtime: {end_time - start_time:.2f} seconds")

print(f"Memory Usage: {psutil.Process().memory_info().rss / (1024 * 1024):.2f} MB")
