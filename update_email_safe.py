import sqlite3
import os

# Safe path handling
db_path = os.path.join("src", "dashboard", "data", "users.db")
print(f"Connecting to: {db_path}")

try:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Check if user exists
    c.execute("SELECT * FROM users WHERE username='neeraj00'")
    user = c.fetchone()
    print(f"User before update: {user}")
    
    # Update
    c.execute("UPDATE users SET email='neerajvinod11@gmail.com' WHERE username='neeraj00'")
    conn.commit()
    
    # Verify
    c.execute("SELECT * FROM users WHERE username='neeraj00'")
    user = c.fetchone()
    print(f"User after update: {user}")
    
    conn.close()
    print("SUCCESS")
except Exception as e:
    print(f"ERROR: {e}")
