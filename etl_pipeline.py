import pandas as pd
import re
from sqlalchemy import create_engine

# db conn parameters
DB_NAME = "toxigen_db"
DB_USER = "postgres"  
DB_PASSWORD = "password"  
DB_HOST = "localhost"
DB_PORT = "5432"

# postgresql connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# extract data
file_path = "../data/toxigen.csv"
df = pd.read_csv(file_path)

# transform data
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

df["generation_method"] = df["generation_method"].astype("category")
df["group"] = df["group"].astype("category")
df["prompt_label"] = df["prompt_label"].astype("category")
df["toxicity_level"] = df["toxicity_level"].astype("category")

# load data into PostgreSQL
df.to_sql("toxigen_data", engine, if_exists="replace", index=False)

print("ETL Pipeline completed successfully! Data loaded into PostgreSQL.")
