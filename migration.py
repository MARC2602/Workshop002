import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv("credentials.env")

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")


df = pd.read_csv("data/the_grammy_awards.csv", delimiter=',')

engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

try:
    df.to_sql("clean_awards", engine, if_exists="replace", index=False)
    print("Succesfull migration")
except Exception as e:
    print(f"Error in migration: {e}")