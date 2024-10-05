import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Cargar credenciales
load_dotenv("../credentials.env")  # Aquí van las credenciales para tu base de datos

def load_data_from_db():
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

    query = "SELECT * FROM clean_awards"
    df = pd.read_sql_query(query, engine)

    # Procesamiento de datos
    drop_columns = ['img', 'winner']
    df = df.drop(columns=drop_columns)

    # Conversión de tipos
    df['published_at'] = pd.to_datetime(df['published_at'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
    df['updated_at'] = pd.to_datetime(df['updated_at'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)

    df = df.astype({col: 'string' for col in df.select_dtypes(include='object').columns})

    df['nominee'] = df['nominee'].fillna('Unknown')
    df['artist'] = df['artist'].fillna('Unknown')
    df['workers'] = df['workers'].fillna('Unknown')

    # Normalización
    df['artist'] = df['artist'].str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)
    df['nominee'] = df['nominee'].str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)
    df = df.rename(columns={'artist': 'artists', 'nominee': 'track_name'})

    return df

def save_to_db(df):
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

    try:
        df.to_sql("clean_awards", engine, if_exists="replace", index=False)
        print("Successful migration")
    except Exception as e:
        print(f"Error in migration: {e}")
