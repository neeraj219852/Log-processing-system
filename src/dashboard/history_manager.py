import sqlite3
import os
import streamlit as st
from datetime import datetime
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "users.db")

def init_history_db():
    """Initialize the analysis history table."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS analysis_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            analysis_date TEXT,
            analysis_time TEXT,
            file_name TEXT,
            num_errors INTEGER,
            num_warnings INTEGER,
            data_path TEXT
        )
    ''')
    conn.commit()
    conn.close()

def add_analysis_record(username, file_name, num_errors, num_warnings, data_path, analysis_dt=None):
    """Add a new analysis record."""
    if analysis_dt is None:
        analysis_dt = datetime.now()
    
    date_str = analysis_dt.strftime("%Y-%m-%d")
    time_str = analysis_dt.strftime("%H:%M:%S")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO analysis_history (username, analysis_date, analysis_time, file_name, num_errors, num_warnings, data_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (username, date_str, time_str, file_name, num_errors, num_warnings, data_path))
        conn.commit()
        return c.lastrowid
    except Exception as e:
        print(f"Error saving history: {e}")
        return None
    finally:
        conn.close()

def get_history(username=None):
    """Retrieve analysis history, optionally filtered by username."""
    conn = sqlite3.connect(DB_PATH)
    # Return as dataframe for easy display
    query = "SELECT * FROM analysis_history"
    params = []
    if username:
        query += " WHERE username = ?"
        params.append(username)
    
    query += " ORDER BY id DESC"
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

def get_analysis_data_path(record_id):
    """Get the parquet path for a specific record."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT data_path FROM analysis_history WHERE id = ?", (record_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None
