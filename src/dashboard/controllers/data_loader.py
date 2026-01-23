import streamlit as st
import pandas as pd
import os
import glob
from datetime import datetime, timedelta

def get_latest_mtime(raw_dir: str = "data/raw_logs") -> float:
    """Get the latest modification timestamp from raw logs"""
    try:
        if not os.path.exists(raw_dir): return 0.0
        files = glob.glob(os.path.join(raw_dir, "*.csv"))
        if not files: return 0.0
        return max(os.path.getmtime(f) for f in files)
    except Exception:
        return 0.0

@st.cache_data(show_spinner=False, ttl=300) # Optional TTL for safety
def load_raw_data_v2(last_modified: float, data_dir: str = "data/processed") -> pd.DataFrame:
    """Load processed log data with Memory + Parquet Caching"""
    try:
        parquet_path = os.path.join(data_dir, "analytics_cache.parquet")
        
        # Internal Cache Logic
        # Even if Streamlit cache misses (e.g. restart), check if we can reuse Parquet
        use_parquet = False
        if os.path.exists(parquet_path):
             # Check if Parquet is fresh enough vs the requested last_modified
             # If Parquet mtime >= last_modified, it contains the latest data
             try:
                 parquet_mtime = os.path.getmtime(parquet_path)
                 if parquet_mtime >= last_modified:
                     use_parquet = True
             except: pass
        
        if use_parquet:
            try:
                 df = pd.read_parquet(parquet_path)
                 if 'timestamp' in df.columns:
                      return df.sort_values('timestamp', ascending=False)
                 return df
            except Exception:
                 pass # Fallback to re-process

        # Fallback to CSV
        csv_dir = "data/raw_logs"
        if not os.path.exists(csv_dir):
            return pd.DataFrame()
            
        all_files = glob.glob(os.path.join(csv_dir, "*.csv"))
        if not all_files:
            return pd.DataFrame()
            
        df_list = []
        for filename in all_files:
            try:
                df = pd.read_csv(filename, header=0, quotechar='"')
                df = process_log_dataframe(df)
                if not df.empty:
                    df_list.append(df)
            except Exception: 
                continue
                
        if not df_list: return pd.DataFrame()
        final_df = pd.concat(df_list, ignore_index=True)
        
        # Cache to Parquet for future speedups
        try:
             save_path = os.path.join("data", "processed")
             if not os.path.exists(save_path):
                 os.makedirs(save_path)
             
             # Save as a single optimized file
             parquet_file = os.path.join(save_path, "analytics_cache.parquet")
             final_df.to_parquet(parquet_file)
        except Exception as e:
             print(f"Failed to cache parquet: {e}")

        return final_df.sort_values('timestamp', ascending=False) if 'timestamp' in final_df.columns else final_df
    except Exception as e:
        return pd.DataFrame()

def load_data_from_stream(file_or_files) -> pd.DataFrame:
    """Load and process data directly from one or more uploaded file streams"""
    try:
        if not file_or_files:
            return pd.DataFrame()
        
        # Ensure it's a list for uniform processing
        uploaded_files = file_or_files if isinstance(file_or_files, list) else [file_or_files]
        df_list = []
        
        for uploaded_file in uploaded_files:
             try:
                # Seek to start if reused (though streamlit file buffer usually handled fresh)
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, header=0, quotechar='"')
                df = process_log_dataframe(df)
                if not df.empty:
                    df_list.append(df)
             except Exception:
                 continue
        
        if not df_list:
             return pd.DataFrame()
             
        final_df = pd.concat(df_list, ignore_index=True)

        if not final_df.empty and 'timestamp' in final_df.columns:
             return final_df.sort_values('timestamp', ascending=False)
        return final_df
    except Exception as e:
        st.error(f"Error parsing file(s): {e}")
        return pd.DataFrame()

def process_log_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply standard log parsing and cleaning logic to a raw dataframe"""
    try:
        df.columns = df.columns.str.lower()
        # Remove duplicate columns to prevent index errors
        df = df.loc[:, ~df.columns.duplicated()]
        
        if 'timestamp' not in df.columns:
            # Linux Logs: Month, Date, Time (No Year)
            if 'month' in df.columns and 'date' in df.columns and 'time' in df.columns:
                try:
                    # Construct string: "2025 Jun 14 15:16:01"
                    # Default year 2025
                    current_year = "2025"
                    combined_linux = current_year + " " + df['month'].astype(str) + " " + df['date'].astype(str) + " " + df['time'].astype(str)
                    # Parse
                    df['timestamp'] = pd.to_datetime(combined_linux, format='%Y %b %d %H:%M:%S', errors='coerce')
                except Exception: pass

            # Spark/Windows Logs: Date, Time
            if 'timestamp' not in df.columns and 'date' in df.columns and 'time' in df.columns:
                try:
                    # Normalize columns to string and strip whitespace
                    d_str = df['date'].astype(str).str.strip().str.replace('[', '').str.replace(']', '')
                    t_str = df['time'].astype(str).str.strip().str.replace('[', '').str.replace(']', '').str.replace(',', '.') 
                    combined = d_str + ' ' + t_str
                    
                    # Try parsing with multiple formats
                    # 1. Standard with milliseconds: 2016-09-28 04:30:30.123
                    df['timestamp'] = pd.to_datetime(combined, format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
                    
                    # 2. Standard without milliseconds: 2016-09-28 04:30:30
                    mask = df['timestamp'].isna()
                    if mask.any():
                        df.loc[mask, 'timestamp'] = pd.to_datetime(combined[mask], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                            
                    # 3. Short Year with slashes: 17/06/09 20:10:40 (Spark)
                    mask = df['timestamp'].isna()
                    if mask.any():
                        df.loc[mask, 'timestamp'] = pd.to_datetime(combined[mask], format='%y/%m/%d %H:%M:%S', errors='coerce')

                    # 4. Fallback to generic parser for any remaining (suppress warning if possible)
                    mask = df['timestamp'].isna()
                    if mask.any():
                        try:
                            df.loc[mask, 'timestamp'] = pd.to_datetime(combined[mask], errors='coerce')
                        except: pass

                except Exception as e: 
                    pass
        
        # Normalize Log Levels
        if 'level' in df.columns: 
            df.rename(columns={'level': 'log_level'}, inplace=True)
        
        if 'log_level' not in df.columns:
            df['log_level'] = "UNKNOWN"
        
        df['log_level'] = df['log_level'].fillna("UNKNOWN").astype(str).str.upper()
        df['log_level'] = df['log_level'].replace({'WARNING': 'WARN', 'COMBO': 'INFO'})

        if 'content' in df.columns: df.rename(columns={'content': 'message'}, inplace=True)
        if 'eventtemplate' in df.columns: df.rename(columns={'eventtemplate': 'error_type'}, inplace=True)
        
        if 'timestamp' in df.columns:
            # Use robust parsing for mixed formats (handles newlogs.csv)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', dayfirst=False, errors='coerce')
            df = df.dropna(subset=['timestamp'])
            
        return df
    except Exception:
        return pd.DataFrame()

def filter_data(df: pd.DataFrame, date_range, search_query: str, selected_levels: list, service_source: str) -> pd.DataFrame:
    """Apply filters"""
    if df.empty: return df
    filtered_df = df.copy()
    
    # Time Range
    if date_range and len(date_range) == 2:
        start_date, end_date = date_range
        # Convert to datetime64[ns] to match df
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date) + timedelta(days=1) - timedelta(seconds=1)
        if 'timestamp' in filtered_df.columns:
            filtered_df = filtered_df[(filtered_df['timestamp'] >= start_ts) & (filtered_df['timestamp'] <= end_ts)]
        
    # Log Levels
    if 'log_level' in filtered_df.columns and selected_levels:
        # Filter if selected, if none selected imply ALL? Or None? Usually ALL if empty or check "All". 
        # But here we have explicit checkboxes.
        if selected_levels:
            # Case insensitive
            filtered_df = filtered_df[filtered_df['log_level'].astype(str).str.upper().isin(selected_levels)]
            
    # Search (now exact match via dropdown)
    if search_query and search_query != "All":
        # Check if we should filter by error_type or message
        # We assume if the user selected something, it came from the list generated in render_filters
        target_col = 'error_type' if 'error_type' in filtered_df.columns else 'message'
        
        if target_col in filtered_df.columns:
             filtered_df = filtered_df[filtered_df[target_col] == search_query]
        
    return filtered_df
