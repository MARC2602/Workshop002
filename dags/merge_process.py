import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import logging

def load_credentials():
    load_dotenv("../credentials.env")

def login_drive():
    credentials_drive = "../client_secrets.json"  # Reemplaza con la ruta a tu archivo de credenciales
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_drive
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_drive)
    
    if gauth.credentials is None:
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
        
    gauth.SaveCredentialsFile(credentials_drive)
    credentials = GoogleDrive(gauth)
    return credentials

def upload_to_drive(merge_path, folder):
    credentials = login_drive()
    file = credentials.CreateFile({'parents': [{"kind": "drive#fileLink", 'id': folder}]})
    file['title'] = merge_path.split('/')[-1]
    file.SetContentFile(merge_path)
    file.Upload()
    logging.info(f'The file {merge_path} has been successfully uploaded to Google Drive')

def merge_data(spotify_df, awards_df):
    # Realizar el merge
    merged_df = pd.merge(spotify_df, awards_df, on=['artists', 'track_name'], how='left')
    return merged_df

def load_to_db(merged_df):
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

    # Guardar en la base de datos
    try:
        merged_df.to_sql("transformed_dataset", engine, if_exists="replace", index=False)
        print("Successful migration")
    except Exception as e:
        print(f"Error in migration: {e}")

def merge_and_store(spotify_df, awards_df):
    merged_df = merge_data(spotify_df, awards_df)
    
    # Guardar el CSV localmente
    merged_path = "../data/transformed_dataset.csv"
    merged_df.to_csv(merged_path, index=False)

    # Cargar en la base de datos
    load_to_db(merged_df)

    # Subir a Google Drive (especifica el ID de la carpeta en Drive donde deseas guardar el archivo)
    upload_to_drive(merged_path, '1o_7Fdw4s-Yhv_bp99vNamVY8t1v1ny79')  # Reemplaza con el ID de tu carpeta en Google Drive
