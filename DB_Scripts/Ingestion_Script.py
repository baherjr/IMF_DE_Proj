import sqlite3
import pandas as pd

conn = sqlite3.connect('../IMF_DB.db')

value_df = pd.read_csv(r'../IMF-Data/values.csv')
indicators_df = pd.read_csv(r'../IMF-Data/indicators.csv')

def ingest_imf_values():
    with conn:
        value_df.to_sql('imf_values', conn, if_exists='replace', index= False)

def ingest_imf_indicators():
    with conn:
        indicators_df.to_sql('imf_indicators', conn, if_exists='replace', index= False)

