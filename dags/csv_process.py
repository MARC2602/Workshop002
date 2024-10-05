import pandas as pd

def read_csv():
    df = pd.read_csv("../data/spotify_dataset.csv", delimiter=',')
    return df

def transform_csv(df):
    # Procesamiento de datos
    to_eliminate = ['Unnamed: 0']
    df = df.drop(columns=to_eliminate)

    df = df.dropna(subset=['artists'])

    df = df.astype({col: 'string' for col in df.select_dtypes(include='object').columns})

    # Normalización
    df['artists'] = df['artists'].str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)
    df['track_name'] = df['track_name'].str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)

    df.to_csv('../data/spotify_dataset_clean.csv', index=False)
    return df
