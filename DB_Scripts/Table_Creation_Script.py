import sqlite3

IMF_TABLE = """CREATE TABLE IF NOT EXISTS imf_values (Country TEXT,Indicator TEXT,Year TEXT,Value NUMERIC);"""
IMF_INDICATORS = """CREATE TABLE IF NOT EXISTS imf_indicators (id TEXT,title TEXT,description TEXT,units TEXT,scale TEXT)"""

conn = sqlite3.connect('../IMF_DB.db')

def create_tables():
    with conn:
        conn.execute(IMF_TABLE)
        conn.execute(IMF_INDICATORS)
