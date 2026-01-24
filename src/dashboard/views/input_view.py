import streamlit as st
import pandas as pd
import os
from datetime import datetime
import history_manager
from controllers.data_loader import load_data_from_stream

def render_input_page():
     # History DB initialized in app.py

    
    """
    Renders the Input Page where users upload their CSV file.
    Matches the design: Centered, Clean title, Input bar, Analyse button.
    """
    
    # Use columns to center the layouts
    # Center Vertically is hard in Streamlit without custom components or lots of spacers.
    # We will use 'st.write' with spacers.
    
    st.markdown('<div style="height: 15vh;"></div>', unsafe_allow_html=True)
    
    # Specific Background for Input Page
    # Specific Background for Input Page
    theme = st.session_state.get("theme_mode", "Light")
    bg_color = "#F3F4F4" if theme == "Light" else "#061E29"
    
    st.markdown(f"""
        <style>
            .stApp {{
                background-color: {bg_color} !important;
            }}
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="input-title">LOG PROCESSING SYSTEM</div>', unsafe_allow_html=True)
    st.markdown('<div class="input-subtitle">Securely analyze and process your system logs</div>', unsafe_allow_html=True)
    
    st.markdown('<div style="height: 2vh;"></div>', unsafe_allow_html=True)
    
    # Layout: [Spacer 1] [Uploader 2] [Button 0.5] [Spacer 1]
    # We want them close.
    
    c1, c2, c3, c4 = st.columns([1, 4, 1, 1])
    # Fine-tuning centered block:
    # 20% left, 50% uploader, 10% button, 20% right?
    # Let's try [1, 2, 0.5, 1] to keep it tight.
    
    col_spacer_l, col_upload, col_btn, col_spacer_r = st.columns([1, 2.5, 0.8, 1])

    with col_upload:
        uploaded_files = st.file_uploader(
            "Upload files", 
            type=["csv"], 
            help="Upload your log files here",
            label_visibility="collapsed",
            accept_multiple_files=True
        )
        
    with col_btn:
        # Align button to bottom or center relative to file uploader?
        # File uploader has height. Button is short.
        # Add some padding to push button down to match uploader box if possible.
        st.markdown('<div style="height: 4px;"></div>', unsafe_allow_html=True) 
        analyze_clicked = st.button("Analyse ➔", type="primary", use_container_width=True)

    
    st.markdown('<div class="helper-text">File size < 200MB (per file)</div>', unsafe_allow_html=True)
    
    st.markdown('<div class="footer-powered">POWERED BY PYSPARK</div>', unsafe_allow_html=True)

    # Logic
    if analyze_clicked:
        if uploaded_files:
            # Validate size (check each file)
            # Ensure list if single file (though accept_multiple_files=True returns list always if not empty)
            files = uploaded_files if isinstance(uploaded_files, list) else [uploaded_files]
            
            oversized = [f.name for f in files if f.size > 200 * 1024 * 1024]
            if oversized:
                st.error(f"File size exceeds 200MB limit: {', '.join(oversized)}")
            else:
                with st.spinner(f"Processing {len(files)} file(s)..."):
                    # Process
                    df = load_data_from_stream(files)
                    
                    if not df.empty:
                        # Success
                        st.session_state['log_data'] = df
                        st.session_state['data_ready'] = True
                        
                        # --- History Recording ---
                        try:
                            # 1. Metrics
                            # Check column names lower case as per data_loader
                            num_errors = len(df[df['log_level'] == 'ERROR']) if 'log_level' in df.columns else 0
                            num_warnings = len(df[df['log_level'] == 'WARN']) if 'log_level' in df.columns else 0
                            
                            # 2. File Name display
                            display_name = ", ".join([f.name for f in files]) if len(files) < 3 else f"{len(files)} files"
                            if len(display_name) > 50: display_name = display_name[:47] + "..."
                            
                            # 3. Save Data
                            hist_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "history")
                            os.makedirs(hist_dir, exist_ok=True)
                            
                            timestamp_id = datetime.now().strftime("%Y%m%d_%H%M%S")
                            parquet_name = f"analysis_{timestamp_id}.parquet"
                            parquet_path = os.path.join(hist_dir, parquet_name)
                            
                            df.to_parquet(parquet_path)
                            
                            # 4. DB Record
                            username = st.session_state.get('username', 'Anonymous')
                            history_manager.add_analysis_record(
                                username, display_name, num_errors, num_warnings, parquet_path
                            )
                        except Exception as e:
                            print(f"Failed to record history: {e}")
                            # Don't block the user flow for history failure, just log it.

                        st.toast("Analysis Complete!", icon="✅")
                        st.rerun()
                    else:
                        st.error("Failed to parse the files. Please ensure they are valid CSV log files.")
        else:
            st.warning("Please upload at least one file.")
