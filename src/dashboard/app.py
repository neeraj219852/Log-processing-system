"""
Streamlit Dashboard for Log Processing System
Modern, interactive dashboard for visualizing log analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import json
import glob
from pathlib import Path
import alerts
import auth
import otp_utils

# --- CONFIGURATION & STYLING ---
# Page configuration
st.set_page_config(
    page_title="Distributed Log Analytics",
    layout="wide",
    initial_sidebar_state="collapsed" # Hide default sidebar
)

# Custom CSS for "Clean Corporate/Enterprise" Light UI
def load_css(file_name):
    # Base CSS
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    
    # Dynamic Theme Overrides
    
    theme_mode = st.session_state.get("theme_mode", "Light")
    primary_color = st.session_state.get("primary_color", "#0D9488")
    
    # 2. Primary Color Global Override
    color_css = f"""
    :root {{
        --primary-color: {primary_color};
    }}
    """
    
    
    # 1. Dark Mode Variables (Override CSS Variables)
    dark_css = ""
    if theme_mode == "Dark":
        dark_css = """
        :root {
            /* Main Background: Deep Blue-Green / Dark Teal */
            --bg-color: #0B1215; 
            
            /* Secondary/Sidebar: Slightly lighter/different tone */
            --bg-secondary: #0F191E;
            
            /* Cards: Lighter shade of the background for depth */
            --card-bg: #162226;
            
            /* Text: Soft Off-White (Primary) & Muted Cyan/Gray (Secondary) */
            --text-title: #ECFEFF; /* Cyan 50 */
            --text-body: #CFFAFE; /* Cyan 100 */
            --text-muted: #67E8F9; /* Cyan 300 - acting as muted/secondary */
            
            /* Borders: Subtle, low-contrast */
            --border-color: #20353B;
            --border-hover: #29454D;
            
            /* Shadows: Soft glow effect */
            --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.5);
            --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.6);
            
            /* Accents overridden for Dark Mode visibility */
            --color-success: #34D399; /* Emerald 400 (Soft Green) */
            --color-warning: #FBBF24; /* Amber 400 (Low saturation yellow-orange) */
            --color-error: #F87171;   /* Red 400 (Muted Red) */
            --color-info: #38BDF8;    /* Sky 400 */
            
            /* Backgrounds for Badges/Alerts */
            --bg-success-light: rgba(52, 211, 153, 0.15);
            --bg-warning-light: rgba(251, 191, 36, 0.15);
            --bg-error-light: rgba(248, 113, 113, 0.15);
            --bg-info-light: rgba(56, 189, 248, 0.15);
        }
        
        /* Dashboard Specific Overrides */
        .stApp { 
            background-color: var(--bg-color); 
            color: var(--text-body); 
        }
        
        /* Force Plotly Charts to be transparent to blend */
        .js-plotly-plot .plotly .main-svg {
            background-color: transparent !important;
        }
        
        /* Streamlit Input Widgets Dark Styling */
        div[data-baseweb="select"] > div, 
        div[data-baseweb="input"] > div, 
        div[data-baseweb="base-input"] {
            background-color: #0F191E !important;
            border-color: #20353B !important;
            color: #ECFEFF !important;
        }
        
        /* Dropdown options */
        div[data-baseweb="popover"], div[data-baseweb="menu"] {
            background-color: #162226 !important;
            border-color: #20353B !important;
        }
        
        input { color: #ECFEFF !important; }
        
        /* Fix Invisible Labels in Dark Mode */
        label, .stMarkdown p, .stRadio label, .stCheckbox label, .stMultiSelect label, .stTextInput label, .stNumberInput label {
            color: var(--text-body) !important;
        }
        
        /* Specific Override for Radio/Checkbox Group Options */
        div[role="radiogroup"] label p, 
        div[data-baseweb="checkbox"] label p,
        div[data-testid="stCheckbox"] label p {
            color: var(--text-body) !important;
        }

        /* Fix Secondary Buttons (Export, View, etc.) - Make them Dark Teal */
        button[kind="secondary"], 
        button[data-testid="baseButton-secondary"],
        a[data-testid="stDownloadButton"] {
            background-color: #162226 !important;
            color: #ECFEFF !important;
            border: 1px solid #20353B !important;
        }
        
        button[kind="secondary"]:hover, 
        button[data-testid="baseButton-secondary"]:hover,
        a[data-testid="stDownloadButton"]:hover {
            border-color: #67E8F9 !important;
            color: #67E8F9 !important;
            background-color: #0F191E !important;
        }

        /* Dialog/Modal Styling for Dark Mode */
        div[role="dialog"],
        div[data-testid="stDialog"] > div {
            background-color: #162226 !important;
            color: #ECFEFF !important;
            border: 1px solid rgba(103, 232, 249, 0.2) !important; /* Subtle Cyan Tint */
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.5), 0 0 15px rgba(13, 148, 136, 0.15) !important; /* Depth + Glow */
            border-radius: 12px !important;
        }
        
        div[role="dialog"] h2,
        div[role="dialog"] h3,
        div[role="dialog"] p,
        div[role="dialog"] div,
        div[role="dialog"] span,
        div[role="dialog"] button[aria-label="Close"] {
            color: #ECFEFF !important;
        }
        
        /* Fix Close Button Icon specifically if needed */
        button[aria-label="Close"] svg {
            fill: #ECFEFF !important;
            color: #ECFEFF !important;
        }

        /* HEADER STYLING FOR DARK MODE */
        header[data-testid="stHeader"] {
            background-color: var(--bg-color) !important;
            border-bottom: 1px solid var(--border-color);
        }

        /* POPOVER DARK MODE SUPPORT */
        /* POPOVER DARK MODE SUPPORT */
        /* POPOVER DARK MODE SUPPORT - TARGETED */
        [data-testid="stPopoverBody"] {
            background-color: #162226 !important;
            border: 1px solid #20353B !important;
            min-width: 350px !important;
        }
        
        /* Ensure text is visible in the container */
        [data-testid="stPopoverBody"] > div {
             color: #ECFEFF !important;
        }

        /* Re-assert input/select backgrounds inside popover just in case */
        [data-testid="stPopoverBody"] input,
        [data-testid="stPopoverBody"] div[data-baseweb="select"] > div {
             background-color: #0F191E !important;
             border-color: #20353B !important;
        }

        /* Attractive MultiSelect Tags in Dark Mode */
        [data-testid="stPopoverBody"] span[data-baseweb="tag"] {
            background-color: rgba(13, 148, 136, 0.3) !important; /* Teal transparent */
            border: 1px solid #2DD4BF !important; /* Teal 400 */
        }
        [data-testid="stPopoverBody"] span[data-baseweb="tag"] span {
            color: #CCFBF1 !important; /* Teal 100 */
            font-weight: 600 !important;
        }
        
        /* Fix Header Icons/Text Visibility */
        header[data-testid="stHeader"] .st-emotion-cache-15w659t, /* Generic class catch (fragile but helper) */
        header[data-testid="stHeader"] button,
        header[data-testid="stHeader"] svg,
        header[data-testid="stHeader"] span {
            color: var(--text-body) !important;
            fill: var(--text-body) !important;
        }

        /* Scrollbar styling for dark mode */
        /* Force Scrollbar to prevent layout shift */
        div[data-testid="stAppViewContainer"] {
            overflow-y: scroll;
        }

        ::-webkit-scrollbar {
          width: 8px;
          height: 8px;
        }
        ::-webkit-scrollbar-track {
          background: #0B1215; 
        }
        ::-webkit-scrollbar-thumb {
          background: #20353B; 
          border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
          background: #29454D; 
        }
        /* Sidebar Styling for Dark Mode */
        section[data-testid="stSidebar"] {
            background: #5F9598 !important; /* Muted Teal for Dark Mode */
            border-right: 1px solid rgba(255, 255, 255, 0.1);
            box-shadow: 2px 0 10px rgba(0,0,0,0.2);
        }
        
        /* Force all text in sidebar to be visible */
        section[data-testid="stSidebar"] .stMarkdown,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] div {
            color: var(--text-body) !important;
        }
        
        section[data-testid="stSidebar"] h1, 
        section[data-testid="stSidebar"] h2, 
        section[data-testid="stSidebar"] h3 {
            color: var(--text-title) !important;
        }

        /* Fix User Profile Text specifically if mostly bespoke HTML */
        section[data-testid="stSidebar"] h3 {
            color: #F8FAFC !important; /* Ensuring the profile name is bright white/slate-50 */
        }

        /* Sidebar specific scrollbar adjustments if needed */
        section[data-testid="stSidebar"] ::-webkit-scrollbar-track {
            background: var(--bg-secondary);
        }
        
        /* File Uploader Uploaded File Name Fix */
        [data-testid="stFileUploader"] div div div small,
        [data-testid="stFileUploaderFileName"] {
            color: #ECFEFF !important;
        }
        """

    st.markdown(f'<style>{dark_css}{color_css}</style>', unsafe_allow_html=True)

# Helper Functions (Loaders)


# --- IMPORTS ---
try:
    from views.auth_view import login_page
    from views.settings_view import render_settings
    from views.input_view import render_input_page
    from views import dashboard_view, search_view
    from controllers.data_loader import load_raw_data_v2, filter_data
    from components.ui_components import view_error_details, view_alert_history, render_kpi, render_progress_bar, view_analysis_history
    import history_manager
except ImportError:
    # Fallback for direct execution
    import sys
    sys.path.append(str(Path(__file__).parent))
    from views.auth_view import login_page
    from views.settings_view import render_settings
    from views.input_view import render_input_page
    from views import dashboard_view, search_view
    from controllers.data_loader import load_raw_data_v2, filter_data
    from components.ui_components import view_error_details, view_alert_history, render_kpi, render_progress_bar, view_analysis_history
    import history_manager

# --- Authentication UI ---
# (Logic moved to views/auth_view.py)

import extra_streamlit_components as stx

def logout():
    # Clear cookie
    cookie_manager = stx.CookieManager(key="auth_cookie_manager")
    try:
        cookie_manager.delete("auth_username")
    except KeyError:
        pass # Cookie already gone
    
    st.session_state.logged_in = False
    st.session_state.username = None
    # Prevent immediate auto-login on next run
    st.session_state['just_logged_out'] = True
    
    # Clear preferences on logout to avoid stale state for next user
    if "theme_mode" in st.session_state: del st.session_state.theme_mode
    if "primary_color" in st.session_state: del st.session_state.primary_color
    st.rerun()

# --- Helper Functions (Loaders) ---

# (Data logic moved to controllers/data_loader.py)


# --- Report Generation ---

def generate_csv_report(df: pd.DataFrame) -> str:
    """Generate CSV report string from current dataframe"""
    if df.empty: return ""
    
    # 1. Summary Section
    total = len(df)
    errors = len(df[df['log_level'] == 'ERROR']) if 'log_level' in df.columns else 0
    warnings = len(df[df['log_level'] == 'WARN']) if 'log_level' in df.columns else 0
    rate = (errors / total * 100) if total > 0 else 0
    
    summary = f"SUMMARY REPORT\nDeprecated Generated_at,{datetime.now()}\nTotal Logs,{total}\nTotal Errors,{errors}\nTotal Warnings,{warnings}\nError Rate,{rate:.2f}%\n"

    # --- Enhanced Metrics ---
    peak_error_info = "N/A"
    most_freq_error = "N/A"
    
    try:
        if not df.empty and 'log_level' in df.columns and 'timestamp' in df.columns:
             err_df_metrics = df[df['log_level'] == 'ERROR'].copy()
             if not err_df_metrics.empty:
                 # 1. Peak Time (Hourly)
                 try:
                     # Ensure we don't breaks original index if needed, but here we work on copy
                     err_df_metrics = err_df_metrics.set_index('timestamp')
                     hourly_counts = err_df_metrics.resample('h').size()
                     if not hourly_counts.empty:
                         peak_ts = hourly_counts.idxmax()
                         peak_val = hourly_counts.max()
                         peak_end = peak_ts + timedelta(hours=1)
                         peak_error_info = f"{peak_ts.strftime('%Y-%m-%d %H:00')} - {peak_end.strftime('%H:00')} (Count: {peak_val})"
                 except Exception: pass
                 
                 # 2. Frequent Error
                 if 'message' in df.columns: # Use original df columns check just in case
                     try:
                         # We use the filtered err_df_metrics (reset index needed? No, value_counts works on Series)
                         # err_df_metrics index is timestamp now, but column 'message' should still be there?
                         # set_index moves column to index? No, keeps others if not dropped? 
                         # Default behavior: `timestamp` moves to index, `message` remains as column.
                         top_err = err_df_metrics['message'].value_counts()
                         if not top_err.empty:
                             msg = top_err.index[0]
                             count = top_err.iloc[0]
                             # Escape commas for CSV safety
                             msg_clean = str(msg).replace(',', ';').replace('\n', ' ')
                             most_freq_error = f"{msg_clean} (Count: {count})"
                     except Exception: pass
    except Exception: pass

    summary += f"Peak Error Time,{peak_error_info}\nMost Frequent Error,{most_freq_error}\n\n"

    
    # 2. Detailed Data
    # Select useful columns
    cols = [c for c in ['timestamp', 'log_level', 'service', 'error_type', 'message'] if c in df.columns]
    
    # Filter for ERRORS only
    error_df = df[df['log_level'] == 'ERROR'] if 'log_level' in df.columns else pd.DataFrame(columns=cols)
    csv_data = error_df[cols].to_csv(index=False)
    
    return summary + "DETAILED ERROR LOGS\n" + csv_data

def generate_json_report(df: pd.DataFrame) -> str:
    """Generate JSON report string from current dataframe"""
    if df.empty: return "{}"
    
    total = len(df)
    errors = len(df[df['log_level'] == 'ERROR']) if 'log_level' in df.columns else 0
    warnings = len(df[df['log_level'] == 'WARN']) if 'log_level' in df.columns else 0
    rate = (errors / total * 100) if total > 0 else 0
    
    report = {
        "report_meta": {
            "generated_at": str(datetime.now()),
            "total_logs": total,
            "total_errors": errors,
            "total_warnings": warnings,
            "error_rate_percent": round(rate, 2)
        },
        "top_errors": [],
        "logs": []
    }
    
    # Top Errors
    if 'message' in df.columns and 'log_level' in df.columns:
        err_df = df[df['log_level'] == 'ERROR']
        if not err_df.empty:
            top = err_df['message'].value_counts().head(5).to_dict()
            report["top_errors"] = [{"message": k, "count": v} for k, v in top.items()]
            
    # Logs (Limit to top 1000 for size safety or dump all? Let's dump all but be careful with memory)
    # Using records orientation
    cols = [c for c in ['timestamp', 'log_level', 'service', 'error_type', 'message'] if c in df.columns]
    
    # Convert timestamp to str for JSON serialization
    export_df = df[cols].copy()
    if 'timestamp' in export_df.columns:
        export_df['timestamp'] = export_df['timestamp'].astype(str)
    
    # Filter for ERRORS only
    if 'log_level' in export_df.columns:
        export_df = export_df[export_df['log_level'] == 'ERROR']
        
    report["logs"] = export_df.to_dict(orient="records")
    
    return json.dumps(report, indent=2)



# --- Main App ---

def render_filters(df: pd.DataFrame):
    with st.container():
        st.markdown('<div class="filter-panel">', unsafe_allow_html=True)
        # Reset Logic
        def reset_filters():
            st.session_state.filter_mode = "All Time"
            # Reset all level checkbox keys
            for key in st.session_state:
                if key.startswith("log_level_"):
                    st.session_state[key] = True

        if st.session_state.get("trigger_reset"):
            st.session_state.trigger_reset = False
            st.rerun()

        h_col1, h_col2 = st.columns([2, 1])
        with h_col1:
            st.markdown("""
<div class="filter-header-row" style="margin-bottom: 0;">
<div class="filter-main-title">Filters</div>
</div>
""", unsafe_allow_html=True)
        with h_col2:
            # Use standard size button, slightly more width in column
            if st.button("Reset", key="reset_btn", use_container_width=True):
                reset_filters()
                st.session_state.trigger_reset = True
                st.rerun()

        # Date Filter Mode
        st.markdown('<div class="filter-section-title">DATE RANGE</div>', unsafe_allow_html=True)
        # Default to All Time (index 0) to ensure all logs from different years are visible
        if "filter_mode" not in st.session_state:
            st.session_state.filter_mode = "All Time"
            
        filter_mode = st.radio(
            "Filter Mode", 
            ["All Time", "Custom Range"], 
            horizontal=True, 
            label_visibility="collapsed",
            key="filter_mode"
        )

        date_range = None
        if filter_mode == "Custom Range":
            # Determine default range based on data
            if not df.empty and 'timestamp' in df.columns:
                max_ts = df['timestamp'].max()
                default_end = max_ts.date()
                default_start = default_end - timedelta(days=7)
            else:
                default_end = datetime.now().date()
                default_start = default_end - timedelta(days=7)
                
            c_start, c_end = st.columns(2)
            with c_start:
                start_date = st.date_input("Start", value=default_start)
            with c_end:
                end_date = st.date_input("End", value=default_end)
            date_range = (start_date, end_date)
        
        st.markdown('<div class="filter-section-title">LOG LEVELS</div>', unsafe_allow_html=True)
        checks = {}
        
        # Dynamic Log Levels to ensure nothing is hidden
        unique_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
        if not df.empty and 'log_level' in df.columns:
            # Union of standard levels and actual levels in data
            data_levels = df['log_level'].astype(str).dropna().unique().tolist()
            unique_levels = sorted(list(set(unique_levels + data_levels)))
        
        for lvl in unique_levels:
            # Default True for all to show everything by default
            key = f"log_level_{lvl}"
            if key not in st.session_state:
                st.session_state[key] = True
            checks[lvl] = st.checkbox(lvl, key=key)
            
        selected_levels = [lvl for lvl, checked in checks.items() if checked]
        
        # --- Report Export (Replaced Service Source) ---
        st.markdown('<div class="filter-section-title">REPORT EXPORT</div>', unsafe_allow_html=True)
        
        # We need the CURRENT filtered data for the report, but render_filters happens BEFORE filtering in main()
        # So we can't pass the fully filtered DF here easily without circular dependency.
        # Design Choice: The download buttons will trigger a re-run or we accept that we might need to pass the df *into* render_filters 
        # (which we do: 'df' arg is raw data).
        # But wait, user wants to download the ANALYTICS report, likely on the FILTERED data.
        # If we place buttons here, we only have 'df' (raw). 
        # Ideally, we should move these buttons, OR we duplicate the logic, OR we just export the RAW (or partially filtered) data available here?
        # Better UX: Export what I see.
        # BUT: streamlit buttons rerun the script.
        # Let's keep it simple: We will use the 'df' passed in (which is currently RAW in main call).
        # Actually, let's fix the logic in main to pass filtered_df back? No, that's hard.
        # We'll just generate report based on the RAW data loaded (or if we want to support filters, we need to apply them here too).
        # To avoid code duplication, we'll just export the FULL dataset for now as "Analytical Report" often implies full analysis.
        # OR we can apply the local filters (date/level) right here inside render_filters simply for the export?
        # Let's just pass `df` (raw) for now, as re-implementing filter logic inside the view is messy. 
        # Users can filter in excel/json. 
        # UPDATE: User request says "must generate analytics reports... covering summaries".
        # Let's generate based on 'df' (raw) provided to this function.
        
        csv_data = generate_csv_report(df)
        json_data = generate_json_report(df)
        
        c_csv, c_json = st.columns(2)
        with c_csv:
            st.download_button(
                label="CSV",
                data=csv_data,
                file_name=f"analytics_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        with c_json:
            st.download_button(
                label="JSON",
                data=json_data,
                file_name=f"analytics_report_{datetime.now().strftime('%Y%m%d')}.json",
                mime="application/json",
                use_container_width=True
            )
        
        st.markdown('<div class="filter-section-title">ACTIONS</div>', unsafe_allow_html=True)
        
        if st.button("Alert History", use_container_width=True):
            start, end = None, None
            top_errors = []
            
            # Logic to calculate Top Frequent Errors for the selected range/mode
            # We must duplicate some logic because main() hasn't processed filtered_df yet
            temp_df = df.copy()
            
            try:
                if date_range:
                    start, end = date_range
                    s_ts = pd.Timestamp(start)
                    e_ts = pd.Timestamp(end) + timedelta(days=1) - timedelta(seconds=1)
                    if 'timestamp' in temp_df.columns:
                        temp_df = temp_df[(temp_df['timestamp'] >= s_ts) & (temp_df['timestamp'] <= e_ts)]
                
                # Force a focused alert check on this specific view (NO EMAIL)
                alerts.check_alerts(temp_df, force=True, username=st.session_state.username, send_email=False)

                # Extract top errors
                if not temp_df.empty and 'message' in temp_df.columns and 'log_level' in temp_df.columns:
                     err_df_temp = temp_df[temp_df['log_level'] == 'ERROR']
                     if not err_df_temp.empty:
                         # Get top 5 messages
                         top_errors = err_df_temp['message'].value_counts().head(5).index.tolist()

                # Search alerts for this user
                username = st.session_state.username
                if top_errors:
                    view_alert_history(None, None, top_errors, username=username)
                else:
                    view_alert_history(start, end, top_errors, username=username)
            except Exception as e:
                st.error(f"Error accessing alert history: {e}")

        st.markdown('<div style="height: 8px"></div>', unsafe_allow_html=True)

        if st.button("Analysis History 📜", use_container_width=True):
             view_analysis_history(st.session_state.get('username'))

        st.markdown('</div>', unsafe_allow_html=True)

        return date_range, selected_levels


def main():
    # Authentication & Session Management
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    
    if "page" not in st.session_state:
        st.session_state.page = "dashboard"
    
    auth.init_db()
    alerts.init_db()
    history_manager.init_history_db()

    # --- Session Persistence ---
    # Instantiate CookieManager with consistent key
    cookie_manager = stx.CookieManager(key="auth_cookie_manager")
    
    # Check for existing session in cookies if not logged in
    # Note: cookie_manager.get_all() needs time/rerun to populate on first load. 
    # If it returns None initially, we might show login, but next rerun it fixes.
    # To mitigate flickering, we rely on the component's internal state sync.
    cookies = cookie_manager.get_all()
    
    # Startup Sync: Give cookies a chance to load before showing login
    if "startup_sync_done" not in st.session_state:
        st.session_state.startup_sync_done = True
        st.rerun()
    
    if not st.session_state.logged_in and cookies and 'auth_username' in cookies:
        # Check if we explicitly logged out just now
        if not st.session_state.get('just_logged_out'):
            stored_user = cookies['auth_username']
            # Restore session
            st.session_state.logged_in = True
            st.session_state.username = stored_user
            st.session_state.user_email = auth.get_user_email(stored_user)
            st.rerun()
            
    # Reset the logout flag if we reached here (meaning we are showing login page or already logged in)
    if st.session_state.get('just_logged_out') and not st.session_state.logged_in:
        # We are about to show login page, so it's safe to clear this for future restarts
        # keeping it until successful login might be safer, but clearing it here ensures 
        # that if they refresh the page manually later, auto-login can work again if cookie remains.
        # But for now, let's just leave it or clear it. 
        # If we clear it, a manual refresh might auto-login them again if cookie wasn't deleted.
        # But logout() called delete(). So hopefully it's gone by next refresh with user interaction.
        st.session_state['just_logged_out'] = False

    if not st.session_state.logged_in:
        login_page(cookie_manager)
        st.stop()
    
    # --- Theme State Initialization ---
    if st.session_state.logged_in:
        # Load preferences if not set in session (e.g. fresh reload)
        if "theme_mode" not in st.session_state or "primary_color" not in st.session_state:
            prefs = auth.get_preferences(st.session_state.username)
            st.session_state.theme_mode = prefs.get("theme_mode", "Light")
            st.session_state.primary_color = prefs.get("primary_color", "#0D9488")
            
        # Load External CSS (Now that theme_mode is guaranteed)
        css_path = Path("src/dashboard/assets/css/style.css")
        if css_path.exists():
            load_css(str(css_path))
            
    # --- History Loading Logic ---
    if st.session_state.get('trigger_history_load'):
        try:
            hist_id = st.session_state.get('load_history_id')
            data_path = history_manager.get_analysis_data_path(hist_id)
            if data_path and os.path.exists(data_path):
                # Load parquet
                df = pd.read_parquet(data_path)
                st.session_state['log_data'] = df
                st.session_state['data_ready'] = True
                st.session_state['viewing_history'] = True
                st.session_state['history_record_id'] = hist_id
                st.toast("Historical Analysis Loaded", icon="📜")
            else:
                st.error("Failed to load historical data. details not found.")
        except Exception as e:
            st.error(f"Error loading history: {e}")
        
        # Reset trigger
        st.session_state.trigger_history_load = False
    
    # Sidebar: User Profile & Actions
    with st.sidebar:
        st.markdown(f"""
        <div style="padding: 1rem 0; text-align: center;">
            <div style="width: 64px; height: 64px; background: #0D9488; color: white; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.5rem; margin: 0 auto 12px auto;">
                {st.session_state.get('username', 'U')[0].upper()}
            </div>
            <h3 style="margin: 0; color: var(--text-title);">{st.session_state.get('username', 'User')}</h3>
            <p style="margin: 0; color: #94A3B8; font-size: 0.8rem;">Administrator</p>
        </div>
        <hr style="border-color: #334155;">
        """, unsafe_allow_html=True)
        
        # Theme Toggle
        current_theme = st.session_state.get("theme_mode", "Light")
        new_theme = "Dark" if current_theme == "Light" else "Light"
        btn_label = "Dark Mode 🌙" if current_theme == "Light" else "Light Mode ☀️"
        
        if st.button(btn_label, use_container_width=True):
            st.session_state.theme_mode = new_theme
            # Persist
            if st.session_state.username:
                auth.update_preferences(st.session_state.username, new_theme, st.session_state.get("primary_color", "#0D9488"))
            st.rerun()
            
        if st.button("Analysis History 📜", use_container_width=True):
            view_analysis_history(st.session_state.get('username'))
            
        st.markdown('<div style="height: 12px"></div>', unsafe_allow_html=True)

        if st.button("🔍 Search Logs", use_container_width=True, type="secondary" if st.session_state.page != "search" else "primary"):
            st.session_state.page = "search"
            st.rerun()
        
        if st.button("New Analysis 🔄", use_container_width=True):
            st.session_state.data_ready = False
            st.session_state['log_data'] = None
            st.session_state['viewing_history'] = False
            st.session_state.page = "dashboard"
            st.rerun()
            
        st.button("Logout", on_click=logout, type="secondary", use_container_width=True)

    # --- Navigation Logic ---
    if "data_ready" not in st.session_state:
        st.session_state.data_ready = False

    if st.session_state.page == "settings":
        render_settings()
    elif st.session_state.page == "search":
        # Pass raw df to search view (it handles its own isolation)
        data_to_pass = st.session_state.get('log_data')
        search_view.render_search_view(data_to_pass if data_to_pass is not None else pd.DataFrame())
    elif not st.session_state.data_ready:
        render_input_page()
    else:
        # --- Dashboard View ---
        
        # Header
        # Using columns to allow interactive buttons
        h_col_title, h_col_actions = st.columns([6, 1])
        
        with h_col_title:
             st.markdown(f"""
            <div class="header-container" style="border-bottom: none; margin-bottom: 0;">
            <div class="app-title-box">
            <div class="app-icon">⚡</div>
            <div>
            <h1 class="app-title">Log Analytics Dashboard</h1>
            <p class="app-subtitle">Distributed Log Processing System</p>
            </div>
            </div>
            </div>
            """, unsafe_allow_html=True)
            
        with h_col_actions:
            # Custom styled buttons for actions using Streamlit columns
            # We use a little CSS hack in style.css to make these look good or just standard buttons
            ac1, ac2, ac3 = st.columns(3)
            with ac2:
                 if st.button("⚙️", key="nav_settings", help="Settings"):
                     st.session_state.page = "settings"
                     st.rerun()
            with ac3:
                 st.button("👤", key="nav_profile", help="Profile")


        st.markdown('<hr style="margin-top: -10px; margin-bottom: 24px; border-color: #E2E8F0;">', unsafe_allow_html=True)


        # Layout: Main Content (3.5) vs Filters (1)
        col_main, col_filters = st.columns([3.5, 1])
    
        # -- Data Processing --
        # -- Data Processing --
        # Using data from session state (loaded via Input Page)
        df = st.session_state.get('log_data', pd.DataFrame())
        
        if df.empty:
            # Fallback if something went wrong
            st.error("No data available. Please upload a file.")
            st.session_state.data_ready = False
            if st.button("Return to Upload"):
                st.rerun()
            st.stop()
    
        # Initialize and Check Alerts
        # Initialize and Check Alerts
        # Initialize and Check Alerts
        
        user_email = st.session_state.get('user_email')
        
        # Optimize: Only check alerts if data has changed or not checked yet
        current_count = len(df)
        last_count = st.session_state.get('last_alert_check_count', -1)
        
        if current_count != last_count:
            # Skip new alert generation if examining historical data
            if not st.session_state.get('viewing_history'):
                new_alerts = alerts.check_alerts(df, target_email=user_email, username=st.session_state.username)
                if new_alerts:
                    for alert in new_alerts:
                        st.toast(f"⚠️ {alert['message']}")
            st.session_state.last_alert_check_count = current_count
        
        with col_filters:
            time_range, selected_levels = render_filters(df)
            search_query = "All"
        
        # Apply Filters
        filtered_df = filter_data(df, time_range, search_query, selected_levels, "All Services")
    
        
        # Render Dashboard View
        dashboard_view.render_dashboard(filtered_df, container=col_main)

if __name__ == "__main__":
    main()


