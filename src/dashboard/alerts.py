import sqlite3
import pandas as pd
from datetime import datetime
import os
from pathlib import Path

# Use Pathlib for robust path handling (src/dashboard/alerts.py -> ... -> data/alerts.db)
DB_PATH = str(Path(__file__).parent.parent.parent.joinpath("data", "alerts.db"))

def init_db():
    """Initialize the alerts database."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS alert_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            username TEXT,
            alert_type TEXT,
            message TEXT,
            severity TEXT,
            details TEXT,
            email_sent INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def save_alert(alert_type, message, severity, details="", html_body=None, target_email=None, username=None, send_email=True):
    """Save an alert to the database and optionally send an email."""
    email_status = 0
    
    # Email Trigger
    if send_email:
        try:
            if not html_body:
                metrics = {"Message": message, "Severity": severity}
                html_body = create_html_body(f"Alert: {alert_type}", message, metrics, details)
                
            sent = send_email_alert(f"{alert_type} ({severity})", f"{message}\n\n{details}", html_body=html_body, target_email=target_email)
            email_status = 1 if sent else 0
        except Exception:
            email_status = 0

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT INTO alert_history (timestamp, username, alert_type, message, severity, details, email_sent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (datetime.now().isoformat(), username, alert_type, message, severity, details, email_status))
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Failed to save alert: {e}")

def get_alerts(limit=100, start_date=None, end_date=None, username=None):
    """Retrieve alert history with optional date and user filtering."""
    try:
        conn = sqlite3.connect(DB_PATH)
        # Check if username column exists (migration helper)
        try:
            c = conn.cursor()
            c.execute("SELECT username FROM alert_history LIMIT 1")
        except sqlite3.OperationalError:
            # Column missing, we might need to alter table or just ignore for now
            # For this strict requirement, let's return empty if no user column mechanism yet or assume old data is public?
            # User requirement: "user y cannot see history of user x". 
            # If DB is old without username, we should probably hide it or migrate it.
            # Let's simple-migrate in-memory for this session if needed? No, too risky.
            pass

        query = "SELECT * FROM alert_history"
        params = []
        conditions = []
        
        # Strict User Isolation
        if username:
            conditions.append("username = ?")
            params.append(username)
        else:
            # If no username provided, return nothing for safety (or handling system alerts?)
            return pd.DataFrame()

        if start_date:
            conditions.append("timestamp >= ?")
            params.append(pd.Timestamp(start_date).isoformat())
            
        if end_date:
            conditions.append("timestamp <= ?")
            end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            params.append(end_ts.isoformat())
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += f" ORDER BY id DESC LIMIT {limit}"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        print(f"Failed to fetch alerts: {e}")
        return pd.DataFrame()

def check_high_error_rate(df, total, errors, threshold=10, is_in_cooldown=False, target_email=None, top_errors_str="", username=None, send_email=True):
    """Check for high error rate."""
    if total == 0: return None
    
    rate = (errors / total * 100)
    
    if rate > threshold and not is_in_cooldown:
        msg = f"High Error Rate Detected: {rate:.2f}%"
        details = f"Total Logs: {total}\nError Count: {errors}\n{top_errors_str}"
        metrics = {"Total Logs": total, "Error Count": errors, "Error Rate": f"{rate:.2f}%"}
        html = create_html_body("High Error Rate Detected", msg, metrics, top_errors_str)
        
        save_alert("High Error Rate", msg, "Critical", details, html_body=html, target_email=target_email, username=username, send_email=send_email)
        return {"message": msg, "severity": "Critical"}
    return None

def check_critical_rate(df, total, top_errors_str="", threshold=10, is_in_cooldown=False, target_email=None, username=None, send_email=True):
    """Check for high critical log rate."""
    if total == 0: return None
    
    criticals = len(df[df['log_level'] == 'CRITICAL']) if 'log_level' in df.columns else 0
    crit_rate = (criticals / total * 100)
    
    if crit_rate > threshold and not is_in_cooldown:
        msg = f"Critical Log Rate Exceeds {threshold}%: {crit_rate:.2f}%"
        details = f"Total: {total}, Criticals: {criticals}\n{top_errors_str}"
        metrics = {"Total Logs": total, "Critical Logs": criticals, "Critical Rate": f"{crit_rate:.2f}%"}
        html = create_html_body("Critical Log Spike", msg, metrics, top_errors_str)
        
        save_alert("High Critical Rate", msg, "Critical", details, html_body=html, target_email=target_email, username=username, send_email=send_email)
        return {"message": msg, "severity": "Critical"}
    return None

def check_frequent_patterns(df, errors, is_in_cooldown=False, target_email=None, username=None, send_email=True):
    """Check for frequent error patterns and bursts."""
    triggered = []
    
    if 'message' not in df.columns or 'log_level' not in df.columns:
        return triggered

    err_df = df[df['log_level'] == 'ERROR'].copy()
    if err_df.empty:
        return triggered

    # Batch Frequency Check (> 5 occurrences)
    error_counts = err_df['message'].value_counts()
    freq_errors = error_counts[error_counts > 5]
    
    if not freq_errors.empty and not is_in_cooldown:
        count_of_patterns = len(freq_errors)
        top_pattern = freq_errors.index[0]
        top_count = freq_errors.iloc[0]
        
        msg = f"Frequent Error Detected: {top_pattern} ({top_count} times)"
        if count_of_patterns > 1:
            msg = f"Multiple Frequent Errors Detected ({count_of_patterns} types)"

        details_lines = ["Errors occurring > 5 times:"]
        for err_msg, count in freq_errors.items():
            details_lines.append(f"- {err_msg}: {count} occurrences")
        details = "\n".join(details_lines)
        
        metrics = {
            "Unique Frequent Errors": count_of_patterns,
            "Top Error Count": top_count,
            "Total Errors in Batch": errors
        }
        
        html = create_html_body("Frequent Error Patterns Detected", msg, metrics, details)
        save_alert("Frequent Error Pattern", msg, "Critical", details, html_body=html, target_email=target_email, username=username, send_email=send_email)
        triggered.append({"message": msg, "severity": "Critical"})

    # Burst Check (> 20 occurrences in 1 Hour)
    # Ensure valid timestamp index
    if 'timestamp' in err_df.columns:
         # Clean timestamps
         err_df['timestamp'] = pd.to_datetime(err_df['timestamp'], errors='coerce')
         err_df = err_df.dropna(subset=['timestamp'])
         
         if not err_df.empty:
             # Identify messages with > 20 occurrences *total* first to filter
             potential_msgs = error_counts[error_counts > 20].index.tolist()
             
             high_freq_triggered = False
             for target_msg in potential_msgs:
                 if high_freq_triggered: break # Avoid spamming multiple alerts for same burst
                 
                 sub_df = err_df[err_df['message'] == target_msg].sort_values('timestamp')
                 # Check rolling count
                 try:
                     rolling_counts = sub_df.set_index('timestamp').rolling('1h').count()
                     
                     if not rolling_counts.empty and rolling_counts['message'].max() > 20:
                          max_burst = int(rolling_counts['message'].max())
                          msg = f"Alert: Error Burst Detected - '{target_msg}' ({max_burst}/hr)"
                          details = f"Error '{target_msg}' occurred {max_burst} times in a single hour window."
                          
                          metrics = {"Burst Rate": f"{max_burst}/hr", "Error Message": target_msg}
                          html = create_html_body("Error Burst Detected", msg, metrics, details)
                          
                          save_alert("Error Burst", msg, "Critical", details, html_body=html, target_email=target_email, username=username, send_email=send_email)
                          triggered.append({"message": msg, "severity": "Critical"})
                          high_freq_triggered = True
                 except Exception: pass

    return triggered

def check_alerts(df: pd.DataFrame, force=False, target_email=None, username=None, send_email=True):
    """
    Analyze dataframe for conditions to trigger alerts.
    Returns a list of triggered alerts (dicts).
    """
    if df.empty: return []
    
    triggered_alerts = []
    
    # Common Data
    top_errors_str = ""
    if 'message' in df.columns and 'log_level' in df.columns:
        err_df = df[df['log_level'] == 'ERROR']
        if not err_df.empty:
            top = err_df['message'].value_counts().head(20)
            top_errors_str = "Top Errors:\n" + "\n".join([f"- {msg} ({count})" for msg, count in top.items()])

    total = len(df)
    errors = len(df[df['log_level'] == 'ERROR']) if 'log_level' in df.columns else 0
    
    # Deduplication / Cooldown Logic
    is_in_cooldown = False
    if not force:
        try:
            last_alerts = get_alerts(limit=1)
            last_alert_time = datetime.min
            if not last_alerts.empty:
                 last_ts_str = last_alerts.iloc[0]['timestamp']
                 last_alert_time = datetime.fromisoformat(last_ts_str)
                 
            time_since_last = (datetime.now() - last_alert_time).total_seconds()
            is_in_cooldown = time_since_last < 3600 
        except Exception:
            is_in_cooldown = False

    # Check Rules
    res1 = check_high_error_rate(df, total, errors, is_in_cooldown=is_in_cooldown, target_email=target_email, top_errors_str=top_errors_str, username=username, send_email=send_email)
    if res1: triggered_alerts.append(res1)
    
    res2 = check_critical_rate(df, total, top_errors_str, is_in_cooldown=is_in_cooldown, target_email=target_email, username=username, send_email=send_email)
    if res2: triggered_alerts.append(res2)
    
    res3 = check_frequent_patterns(df, errors, is_in_cooldown=is_in_cooldown, target_email=target_email, username=username, send_email=send_email)
    if res3: triggered_alerts.extend(res3)

    # Manual Force Check
    if force and not triggered_alerts:
        msg = "Manual Alert History Check"
        triggered_alerts.append({"message": msg, "severity": "Info"})

    return triggered_alerts

# --- Email Sending Logic ---
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
import os
import traceback

# Robust Import Logic for Config
try:
    # Try relative first (package mode)
    from . import email_config
except ImportError:
    try:
        # Try absolute (script mode)
        import email_config
    except ImportError:
        try:
             # Try adding directory to path
             sys.path.append(os.path.dirname(os.path.abspath(__file__)))
             import email_config
        except ImportError as e:
             print(f"CRITICAL: Could not import email_config: {e}")
             email_config = None

def create_html_body(title, message, metrics, top_errors_str):
    """
    Create a professional HTML email body.
    """
    # Parse top errors from string back to list if needed, or just format the string
    # Expected top_errors_str format: "Top Errors:\n- Msg (Count)..."
    # Let's clean it up for HTML
    error_list_html = ""
    if top_errors_str:
        lines = top_errors_str.split('\n')
        # Skip header "Top Errors:" if present
        items = [l for l in lines if l.strip().startswith('-')]
        if items:
            error_list_html = "<ul>" + "".join([f"<li>{item[2:]}</li>" for item in items]) + "</ul>"
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
            .header {{ background-color: #DC2626; color: white; padding: 20px; text-align: center; }}
            .header.info {{ background-color: #2563EB; }}
            .content {{ padding: 20px; background-color: #F9FAFB; }}
            .metrics-table {{ width: 100%; margin-bottom: 20px; border-collapse: collapse; }}
            .metrics-table th, .metrics-table td {{ padding: 10px; border-bottom: 1px solid #eee; text-align: left; }}
            .metrics-table th {{ background-color: #f3f3f3; color: #666; font-size: 0.85em; text-transform: uppercase; }}
            .footer {{ background-color: #f1f1f1; padding: 15px; text-align: center; font-size: 0.8em; color: #666; }}
            h2 {{ margin-top: 0; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>{title}</h1>
            </div>
            <div class="content">
                <h2>{message}</h2>
                <table class="metrics-table">
                    <tr><th>Metric</th><th>Value</th></tr>
                    {''.join([f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in metrics.items()])}
                </table>
                
                <h3>Top Frequent Errors</h3>
                {error_list_html if error_list_html else "<p>No specific error patterns detected.</p>"}
                
                <p style="margin-top: 20px;">
                    <a href="http://localhost:8501" style="background-color: #DC2626; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block;">View Dashboard</a>
                </p>
            </div>
            <div class="footer">
                <p>This is an automated alert from the Log Processing System.</p>
                <p>Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html

def send_email_alert(subject, body, html_body=None, target_email=None):
    """Send an email alert using the configured SMTP server."""
    try:
        sender_email = email_config.SENDER_EMAIL
        sender_password = email_config.SENDER_PASSWORD
        receiver_emails = [target_email] if target_email else email_config.RECEIVER_EMAILS
        smtp_server = email_config.SMTP_SERVER
        smtp_port = email_config.SMTP_PORT

        msg = MIMEMultipart('alternative')
        msg['From'] = sender_email
        msg['To'] = ", ".join(receiver_emails)
        msg['Subject'] = f"[ALERT] {subject}"

        # Attach Plain Text
        part1 = MIMEText(body, 'plain')
        msg.attach(part1)
        
        # Attach HTML if available
        if html_body:
            part2 = MIMEText(html_body, 'html')
            msg.attach(part2)

        # Connect to SMTP Server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        
        # Send Email
        server.sendmail(sender_email, receiver_emails, msg.as_string())
        server.quit()
        
        print(f"Email alert sent to {receiver_emails}")
        return True
    except Exception as e:
        print(f"Failed to send email alert: {e}")
        # Print full traceback for debugging (visible in console)
        traceback.print_exc()
        return False

