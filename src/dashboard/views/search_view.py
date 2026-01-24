import streamlit as st
import pandas as pd
from datetime import datetime
from controllers.search_engine import search_logs


@st.dialog("Search Filters")
def filter_dialog(df, unique_levels):
    
    with st.form("search_filters_form"):
        st.markdown("### Search Filters")

        # Calculate limits
        min_val = None
        max_val = None
        if not df.empty and 'timestamp' in df.columns:
                try:
                    min_val = df['timestamp'].min().date()
                    max_val = df['timestamp'].max().date()
                except: pass

        # Date Range
        if "widget_search_date" not in st.session_state:
            st.session_state.widget_search_date = []

        search_dates = st.date_input(
            "Date Range",
            min_value=min_val,
            max_value=max_val,
            key="widget_search_date"
        )
        
        # Time Range
        lbl_start, lbl_end = "Start Time", "End Time"
        cur_dates = st.session_state.get("widget_search_date", [])
        if cur_dates and isinstance(cur_dates, tuple):
                s_d = cur_dates[0]
                e_d = cur_dates[1] if len(cur_dates) > 1 else cur_dates[0]
                if s_d != e_d:
                    lbl_start = f"Time ({s_d.strftime('%b %d')})"
                    lbl_end = f"Time ({e_d.strftime('%b %d')})"
        
        c_t1, c_t2 = st.columns(2)
        with c_t1:
            start_time = st.time_input(lbl_start, value=None, key="search_start_time")
        with c_t2:
            end_time = st.time_input(lbl_end, value=None, key="search_end_time")

        # Levels
        search_levels = st.multiselect(
            "Log Levels",
            unique_levels,
            default=st.session_state.get("search_filter_levels", []),
            key="search_filter_levels"
        )
        
        # Callback needed here since form is isolated
        def clear_filters_callback():
            st.session_state.search_filter_levels = []
            st.session_state.widget_search_date = []
            st.session_state.search_start_time = None
            st.session_state.search_end_time = None
            
        c_apply, c_clear = st.columns([2, 1])
        with c_apply:
            submitted = st.form_submit_button("Apply Filters", use_container_width=True, type="primary")
        with c_clear:
            # Use on_click callback to safely modify state before rerun
            cleared = st.form_submit_button("Clear", use_container_width=True, type="secondary", on_click=clear_filters_callback)

        if submitted:
            st.rerun()

def render_search_view(df: pd.DataFrame):
    """
    Renders the isolated Search View with Popover Filters.
    """
    
    st.markdown("## Advanced Log Search")
    
    # Defaults
    if "filter_popover_id" not in st.session_state:
        st.session_state.filter_popover_id = 0

    if "search_date_range" not in st.session_state:
        if not df.empty and 'timestamp' in df.columns:
            min_ts = df['timestamp'].min().date()
            max_ts = df['timestamp'].max().date()
            st.session_state.search_date_range = (min_ts, max_ts)
        else:
             st.session_state.search_date_range = []

    unique_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
    if not df.empty and 'log_level' in df.columns:
         data_levels = df['log_level'].dropna().unique().tolist()
         unique_levels = sorted(list(set(unique_levels + data_levels)))
         
    unique_services = []
    if not df.empty and 'service' in df.columns:
        unique_services = sorted(df['service'].dropna().unique().tolist())

    # --- Search Layout ---
    # Col 1: Filter Button (Popover)
    # Col 2: Search Input
    # Col 3: Search Action Button
    
    # --- Search Layout ---
    # Col 1: Search Input (Wide)
    # Col 2: Filter Button (Popover) - Compact
    # Col 3: Search Action Button - Compact
    
    c_input, c_filter, c_btn = st.columns([3.5, 0.75, 1])
    
    filters = {}
    
    # 1. Search Input (Left)
    with c_input:
        query = st.text_input(
            "Keywords", 
            placeholder="Search logs...",
            key="search_query_input",
            label_visibility="collapsed"
        )

    # 2. Filter Button (Center/Right)
    with c_filter:
        # Use a Dialog for filters to allow proper closing on Apply
        if st.button("Filters 🌪️", use_container_width=True, type="secondary"):
            filter_dialog(df, unique_levels)
    # Logic continues outside the button/dialog block 
    # Because dialog updates session state, replay logic here:

    # Assign to filters dict
    filters['levels'] = st.session_state.get("search_filter_levels", [])
    # Default services to all since UI control is removed
    filters['services'] = unique_services
    
    # Handle Date/Time
    d_val = st.session_state.get("widget_search_date", [])
    t_start = st.session_state.get("search_start_time")
    t_end = st.session_state.get("search_end_time")
    
    if d_val and isinstance(d_val, tuple):
        # Determine dates
        s_date = d_val[0]
        e_date = d_val[1] if len(d_val) > 1 else d_val[0]
        
        # Determine times (Default to full day if not specified)
        s_time = t_start if t_start else datetime.min.time()
        e_time = t_end if t_end else datetime.max.time()
        
        # Validate Start < End if same day
        if s_date == e_date and s_time > e_time:
            st.warning("Start Time cannot be after End Time.")
            filters['date_range'] = None
        else:
            start_dt = datetime.combine(s_date, s_time)
            end_dt = datetime.combine(e_date, e_time)
            filters['date_range'] = (start_dt, end_dt)
    else:
        filters['date_range'] = None

    # 3. Search Button (Right)
    with c_btn:
        if st.button("Search", type="primary", use_container_width=True):
             pass 

    st.markdown("<div style='height: 12px'></div>", unsafe_allow_html=True)

    # Execute Search
    with st.spinner("Searching logs..."):
        results = search_logs(df, query, filters)
        
    # Stats
    count = len(results)
    total = len(df)
    
    st.caption(f"Found {count} matches out of {total} logs")
    
    if not results.empty:
        display_cols = ['timestamp', 'log_level', 'service', 'message']
        final_cols = [c for c in display_cols if c in results.columns]
        
        st.dataframe(
            results[final_cols],
            use_container_width=True,
            column_config={
                "timestamp": st.column_config.DatetimeColumn("Time", format="D MMM, HH:mm:ss"),
                "log_level": st.column_config.TextColumn("Level", width="small"),
                "service": "Service",
                "message": st.column_config.TextColumn("Message", width="large")
            },
            height=600
        )
    else:
        st.info("No logs found matching your criteria.")
        
    st.markdown("---")
    if st.button("← Back to Dashboard"):
        st.session_state.page = "dashboard"
        st.rerun()
