import streamlit as st
from datetime import datetime
import textwrap
import auth
import sys
from pathlib import Path

# Add parent dir to path if needed for auth import (fallback)
try:
    import auth
except ImportError:
    sys.path.append(str(Path(__file__).parent.parent))
    import auth

import base64

import base64

import base64

import base64

def render_settings():
    """
    Renders the Settings Page with a layout similar to the provided design.
    """
    
    # --- Custom CSS for File Uploader Minimization ---
    st.markdown("""
    <style>
    /* Compact File Uploader */
    div[data-testid="stFileUploader"] {
        width: 100%;
    }
    div[data-testid="stFileUploader"] section {
        padding: 0;
        min-height: 0;
        background-color: transparent;
        border: none;
    }
    div[data-testid="stFileUploader"] section > div {
        padding-top: 0;
        padding-bottom: 0;
    }
    div[data-testid="stFileUploader"] button[kind="secondary"] {
        width: 100%;
        border-color: #CBD5E1; 
        color: #64748B;
        padding-top: 0.25rem;
        padding-bottom: 0.25rem;
    }
    /* Hide the 'Drag and drop file here' text and limit text */
    div[data-testid="stFileUploader"] span, 
    div[data-testid="stFileUploader"] small,
    section[data-testid="stFileUploaderDropzone"] span,
    section[data-testid="stFileUploaderDropzone"] small {
        display: none !important;
    }
    
    /* Fix Read-Only Input Visibility */
    .stTextInput input:disabled {
        color: #1E293B !important; /* Slate 800 - Very dark gray */
        -webkit-text-fill-color: #1E293B !important;
        opacity: 1 !important;
        background-color: #F1F5F9 !important; /* Slate 100 */
        font-weight: 500 !important;
        cursor: not-allowed;
    }

    /* THEME TOGGLE BUTTONS (Global for Settings Page) */
    div[role="radiogroup"] {
        flex-direction: row;
        gap: 8px;
        background: #F1F5F9;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 4px;
        width: fit-content;
    }

    div[role="radiogroup"] label {
        background-color: transparent !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 6px 20px !important;
        flex: 0 1 auto; /* Allow shrink/grow */
        display: flex;
        align-items: center;
        justify-content: center;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: none !important;
        white-space: nowrap; /* Prevent wrapping */
    }

    /* Hide default circle */
    div[role="radiogroup"] label > div:first-child {
        display: none !important;
    }

    /* Selected State - White Card with Shadow */
    div[role="radiogroup"] label:has(input:checked) {
        background-color: #FFFFFF !important;
        color: #0F172A !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04) !important;
    }
    
    div[role="radiogroup"] label:has(input:checked) p {
        color: #0F172A !important;
        font-weight: 600 !important;
        transform: scale(1.02);
    }

    /* Unselected Hover State */
    div[role="radiogroup"] label:not(:has(input:checked)):hover {
        background-color: rgba(255,255,255,0.6) !important;
    }
    
    div[role="radiogroup"] label:not(:has(input:checked)) p {
        color: #64748B !important;
    }

    /* Icon Styling Override */
    div[role="radiogroup"] label p {
        font-size: 0.9rem !important;
        display: flex;
        align-items: center;
        gap: 6px;
        margin: 0;
        transition: transform 0.2s;
    }

    div[role="radiogroup"] label p::before {
        font-family: 'Material Symbols Outlined';
        font-size: 18px;
        font-weight: normal;
        margin-top: -1px; /* Align vertical */
    }

    /* Specific Icons based on Text content */
    /* Light Mode */
    div[role="radiogroup"] label:first-child p::before {
        content: '\\e518'; /* wb_sunny */
    }
    
    /* Dark Mode */
    div[role="radiogroup"] label:last-child p::before {
        content: '\\e51c'; /* dark_mode */
    }

    </style>
    """, unsafe_allow_html=True)
    
    # Retrieve User Info from Session
    current_username = st.session_state.get("username", "System Administrator")
    current_email = st.session_state.get("user_email", "")
    # Retrieve avatar from session request (persisted base64 string)
    current_avatar = st.session_state.get("user_avatar", None)
    


    # --- Main Layout (Sidebar Menu vs Content) ---
    col_nav, col_content = st.columns([1, 3.5])

    with col_nav:
        if st.button("← Back to Dashboard", key="settings_back_btn", type="secondary", use_container_width=True):
            st.session_state.page = "dashboard"
            st.rerun()
            
        st.markdown(textwrap.dedent("""
        <div style="position: sticky; top: 20px;">
          <div style="font-size: 0.7rem; font-weight: 700; color: #94A3B8; text-transform: uppercase; margin-bottom: 8px; padding-left: 12px;">Settings Menu</div>
          <div class="settings-nav-item active">
            <span class="material-symbols-outlined">settings</span> General
          </div>
          <div class="settings-nav-item">
            <span class="material-symbols-outlined">person</span> Account
          </div>
          <div class="settings-nav-item">
            <span class="material-symbols-outlined">terminal</span> Data Processing
          </div>
          <div class="settings-nav-item">
            <span class="material-symbols-outlined">notifications_active</span> Notifications
          </div>
          <div class="settings-nav-item">
            <span class="material-symbols-outlined">security</span> Security
          </div>
          
          <div style="margin-top: 20px; border-top: 1px solid #E2E8F0; padding-top: 16px;">
            <div style="display: flex; gap: 12px; padding: 8px 12px; align-items: center;">
              <div style="width: 36px; height: 36px; background: rgba(13, 148, 136, 0.1); border-radius: 50%; display: flex; align-items: center; justify-content: center; color: #0D9488;">
                <span class="material-symbols-outlined" style="font-size: 20px;">analytics</span>
              </div>
              <div style="overflow: hidden;">
                <div style="font-size: 0.75rem; font-weight: 700; color: #0F172A;">LogFlow Analytics</div>
                <div style="font-size: 0.65rem; color: #64748B; font-weight: 500;">v2.4.1 Stable</div>
              </div>
            </div>
          </div>
        </div>
        """), unsafe_allow_html=True)

    with col_content:
        # --- Breadcrumbs & Header ---
        st.markdown(textwrap.dedent("""
        <div style="margin-bottom: 24px;">
          <div style="display: flex; align-items: center; gap: 8px; color: var(--text-muted); font-size: 0.85rem; margin-bottom: 8px;">
            <span>Dashboard</span>
            <span class="material-symbols-outlined" style="font-size: 14px;">chevron_right</span>
            <span style="color: var(--text-title); font-weight: 500;">Settings</span>
          </div>
          <h1 style="color: var(--text-title); font-size: 1.8rem; font-weight: 800; margin: 0;">General Settings</h1>
          <p style="color: var(--text-muted); margin-top: 4px;">Configure your system preferences and data processing parameters.</p>
        </div>
        """), unsafe_allow_html=True)
        
        # --- User Profile Section ---
        with st.container():
            st.markdown('<div class="settings-card">', unsafe_allow_html=True)
            st.markdown('<div class="settings-card-title">User Profile</div>', unsafe_allow_html=True)
            
            c_profile_icon, c_profile_inputs = st.columns([1, 4])
            
            with c_profile_icon:
                # --- HIDDEN FILE UPLOADER HACK ---
                if "show_uploader" not in st.session_state:
                    st.session_state.show_uploader = False

                # Determine what to show
                avatar_style = "background: #F1F5F9; display: flex; align-items: center; justify-content: center;"
                icon_content = '<span class="material-symbols-outlined" style="font-size: 32px; color: #94A3B8;">person</span>'
                
                uploaded_file = None
                new_avatar_b64 = None

                if current_avatar:
                     avatar_style = f"background-image: url('data:image/png;base64,{current_avatar}'); background-size: cover; background-position: center;"
                     icon_content = ""
                
                # Render Avatar
                # We use a button that looks like the edit icon to toggle visibility
                c_av_view, c_av_edit = st.columns([1, 0.1])
                with c_av_view:
                     st.markdown(textwrap.dedent(f"""
                    <div style="position: relative; width: 80px; margin: 0 auto 10px auto;">
                      <div style="width: 80px; height: 80px; border-radius: 50%; border: 2px dashed #CBD5E1; overflow: hidden; {avatar_style}">
                        {icon_content}
                      </div>
                    </div>
                    """), unsafe_allow_html=True)
                
                # To avoid the "Directly open open folder" issue which might mean an intrusive UI:
                # We will show the toggle button "Edit Photo".
                # When clicked, we show the uploader, BUT we apply the CSS above to make it minimal.
                
                if st.button("Edit Photo", key="btn_toggle_upload", use_container_width=True):
                    st.session_state.show_uploader = not st.session_state.show_uploader
                    st.rerun()

                # Only show uploader if enabled
                if st.session_state.show_uploader:
                    # Renders as a minimal "Browse files" button due to CSS
                    uploaded_file = st.file_uploader("Upload Avatar", type=['png', 'jpg', 'jpeg'], label_visibility="collapsed")
                    if uploaded_file:
                        bytes_data = uploaded_file.getvalue()
                        new_avatar_b64 = base64.b64encode(bytes_data).decode()
                        # Update preview immediately
                        # We also updated persisted state in the main update loop, but for immediate preview:
                        avatar_style = f"background-image: url('data:image/png;base64,{new_avatar_b64}'); background-size: cover; background-position: center;"
                        icon_content = ""
                        # Optionally auto-hide uploader? No, let user confirm.

            with c_profile_inputs:
                c1, c2 = st.columns(2)
                with c1:
                    new_name = st.text_input("Full Name", value=current_username, disabled=True)
                with c2:
                    new_email = st.text_input("Email Address", value=current_email, disabled=True)
            
            st.markdown('</div>', unsafe_allow_html=True)

        # --- Site Configuration Section ---
        with st.container():
            st.markdown('<div class="settings-card">', unsafe_allow_html=True)
            st.markdown('<div class="settings-card-title">Site Configuration</div>', unsafe_allow_html=True)
            
            c1, c2 = st.columns(2)
            with c1:
                st.text_input("Dashboard Name", value="Distributed Log Analytics Dashboard")
            with c2:
                st.selectbox("System Timezone", ["UTC (Coordinated Universal Time)", "EST (Eastern Standard Time)", "PST (Pacific Standard Time)"], index=0)
            
            st.markdown('</div>', unsafe_allow_html=True)

        # --- Theme Preferences Section ---
        with st.container():
            st.markdown('<div class="settings-card">', unsafe_allow_html=True)
            st.markdown('<div class="settings-card-title">Theme Preferences</div>', unsafe_allow_html=True)
            
            # Interface Mode
            c_lbl, c_ctrl = st.columns([1, 1])
            with c_lbl:
                st.markdown("**Interface Mode**")
                st.caption("Toggle between light and dark display modes.")
            with c_ctrl:
                # Key bound directly to session state
                current_theme = st.session_state.get("theme_mode", "Light")
                theme_idx = 1 if current_theme == "Dark" else 0
                
                selected_theme = st.radio(
                    "Interface Mode", 
                    ["Light", "Dark"], 
                    index=theme_idx,
                    horizontal=True, 
                    label_visibility="collapsed",
                    key="theme_radio_manual"
                )
                
                if selected_theme != current_theme:
                    st.session_state.theme_mode = selected_theme
                    auth.update_preferences(
                        st.session_state.username,
                        st.session_state.theme_mode,
                        st.session_state.primary_color
                    )
                    st.rerun()

            st.markdown('<div style="height: 1px; background: #F1F5F9; margin: 16px 0;"></div>', unsafe_allow_html=True)

            # Brand Color
            c_lbl2, c_ctrl2 = st.columns([2, 1])
            with c_lbl2:
                st.markdown("**Brand Primary Color**")
                st.caption("Customize the accent color for your dashboard.")
            with c_ctrl2:
                # Functional Color Picker using Columns
                colors = [
                    {"name": "Teal", "hex": "#0D9488"},
                    {"name": "Green", "hex": "#10B981"},
                    {"name": "Amber", "hex": "#F59E0B"},
                    {"name": "Rose", "hex": "#F43F5E"}
                ]
                
                if "primary_color" not in st.session_state:
                    st.session_state.primary_color = "#0D9488"
                
                cols = st.columns(len(colors))
                for idx, color in enumerate(colors):
                    is_selected = st.session_state.primary_color == color['hex']
                    border_style = f"border: 2px solid {color['hex']};" if is_selected else "border: 1px solid #E2E8F0;"
                    box_shadow = f"box-shadow: 0 0 0 2px {color['hex']};" if is_selected else ""
                    
                    with cols[idx]:
                        # Helper to update state
                        def set_color(c=color['hex']):
                            st.session_state.primary_color = c
                            
                        # We use a button with empty label but styled div inside? 
                        # Streamlit buttons don't support HTML inside well.
                        # Alternative: Use a standard button and style it with emoji or plain text, 
                        # but getting a perfect circle is hard without custom component.
                        # HACK: Use a button with a unique key, check if clicked.
                        # To make it look like a circle, we can't easily.
                        # Fallback: Just use the text name or a colored emoji circle if possible.
                        # BETTER: Use st.button("⬤", ...) with colored font? 
                        # Let's use the HTML approach with clickable callback? No, unsafe_allow_html can't trigger python.
                        # We must use st.button.
                        # Let's render a button and style it via CSS? No unique IDs for specific buttons easily.
                        # SOLUTION: Standard st.button with the color NAME, or just a generic 'Select'
                        # Let's try to make it usable first.
                        if st.button("⬤", key=f"btn_col_{idx}", help=color['name']):
                             st.session_state.primary_color = color['hex']
                             auth.update_preferences(
                                st.session_state.username,
                                st.session_state.theme_mode,
                                st.session_state.primary_color
                             )
                             st.rerun()

                        # We inject CSS to color this specific button? Hard.
                        # Let's just show the current selection state visually below?
                        if is_selected:
                             st.markdown(f"<div style='text-align:center; color:{color['hex']}; font-size:0.8rem; font-weight:bold;'>Active</div>", unsafe_allow_html=True)
                
            st.markdown('</div>', unsafe_allow_html=True)

        # --- PySpark Configuration ---
        with st.container():
            st.markdown('<div class="settings-card">', unsafe_allow_html=True)
            st.markdown(textwrap.dedent("""
            <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 20px;">
              <div class="settings-card-title" style="margin-bottom: 0;">PySpark Configuration</div>
              <span style="background: rgba(13, 148, 136, 0.1); color: #0D9488; font-size: 10px; font-weight: 800; padding: 2px 6px; border-radius: 4px; text-transform: uppercase;">Experimental</span>
            </div>
            """), unsafe_allow_html=True)
            
            c1, c2 = st.columns(2)
            with c1:
                st.text_input("Spark Master URL", placeholder="spark://master:7077")
            with c2:
                st.number_input("Log Refresh Interval (seconds)", value=30, min_value=5)

            st.markdown(textwrap.dedent("""
            <div style="margin-top: 16px; display: flex; gap: 12px; padding: 12px; background: rgba(13, 148, 136, 0.05); border-radius: 8px; border: 1px solid rgba(13, 148, 136, 0.2);">
              <span class="material-symbols-outlined" style="color: #0D9488; font-size: 20px;">info</span>
              <div style="font-size: 0.75rem; color: #0F766E; font-weight: 500; line-height: 1.5;">
                Changes to the Spark configuration will require a restart of the streaming job cluster. Ensure no active jobs are processing critical logs before saving.
              </div>
            </div>
            """), unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)

        # --- Footer Actions ---
        st.markdown(textwrap.dedent("""
        <div style="margin-top: 24px; border-top: 1px solid #E2E8F0; padding-top: 24px; display: flex; justify-content: flex-end; gap: 12px;">
        """), unsafe_allow_html=True)
        
        col_actions_spacer, col_actions_btns = st.columns([3, 2])
        with col_actions_btns:
            # Using columns for buttons to align right
            b_cancel, b_save = st.columns(2)
            with b_cancel:
                if st.button("Discard Changes", type="secondary", use_container_width=True):
                    # Reset UI state
                    st.session_state.show_uploader = False
                    st.session_state.page = "dashboard"
                    st.rerun()
            with b_save:
                if st.button("Save Changes", type="primary", use_container_width=True):
                    # 1. Update Profile (Email/Name)
                    current_user = st.session_state.username
                    
                    # If email changed, we might need to update it in DB
                    if new_email != st.session_state.get("user_email", ""):
                        # (Ideally call auth.update_email here if supported, or just ignore for now as it's readonly-ish)
                        st.session_state.user_email = new_email
                    
                    # 2. Persist Theme (Already done via on_change, but re-confirm)
                    auth.update_preferences(
                        current_user,
                        st.session_state.theme_mode, 
                        st.session_state.primary_color
                    )
                    
                    if new_avatar_b64:
                        st.session_state.user_avatar = new_avatar_b64
                    
                    # 3. Notify and Redirect
                    st.toast("Settings saved successfully!", icon="✅")
                    
                    # Reset UI state
                    st.session_state.show_uploader = False
                    st.session_state.page = "dashboard"
                    st.rerun()
