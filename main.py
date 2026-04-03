import os
import shutil
import json
import time
import sqlite3
import schedule
import threading
from datetime import datetime
from flask import Flask, jsonify

# 1. SETUP FLASK
app = Flask(__name__)

# 2. DATABASE LOGIC (Create the table if it doesn't exist)
def init_db():
    conn = sqlite3.connect('mover.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS move_logs 
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                       filename TEXT, source TEXT, dest TEXT, 
                       timestamp TEXT, status TEXT)''')
    conn.commit()
    conn.close()

def log_to_db(filename, src, dest, status):
    conn = sqlite3.connect('mover.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO move_logs (filename, source, dest, timestamp, status) VALUES (?, ?, ?, ?, ?)",
                   (filename, src, dest, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), status))
    conn.commit()
    conn.close()

# 3. CORE FILE MOVING LOGIC
def move_files():
    print(f"[{datetime.now()}] Starting move operation...")
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        # We loop through a list of folder pairs in config.json
        for folder_pair in config.get('folders', []):
            source = folder_pair['source']
            dest = folder_pair['destination']

            if not os.path.exists(source):
                continue

            for file_name in os.listdir(source):
                source_path = os.path.join(source, file_name)
                dest_path = os.path.join(dest, file_name)
                
                try:
                    shutil.move(source_path, dest_path)
                    log_to_db(file_name, source, dest, "SUCCESS")
                    print(f"Moved: {file_name}")
                except Exception as e:
                    log_to_db(file_name, source, dest, f"FAILED: {str(e)}")
    except Exception as e:
        print(f"Config error: {e}")

# 4. API ROUTES (The REST API requirement)
@app.route('/run-move', methods=['GET'])
def manual_trigger():
    move_files()
    return jsonify({"message": "Manual move triggered!"})

@app.route('/logs', methods=['GET'])
def get_logs():
    conn = sqlite3.connect('mover.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM move_logs ORDER BY id DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()
    return jsonify(rows)

# 5. THE SCHEDULER THREAD (The 10-minute loop)
def run_scheduler():
    schedule.every(10).minutes.do(move_files)
    while True:
        schedule.run_pending()
        time.sleep(1)

# 6. EXECUTION BLOCK
if __name__ == "__main__":
    init_db()
    
    # This starts the 10-minute timer in the BACKGROUND
    # so it doesn't stop the API from working
    threading.Thread(target=run_scheduler, daemon=True).start()
    
    print("Service Started!")
    print("API available at: http://127.0.0.1:5000/run-move")
    
    # This starts the WEB API
    app.run(port=5000, debug=False, use_reloader=False)
   
   