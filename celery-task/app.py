from flask import Flask, jsonify, request
from celery import Celery
import pymysql
from datetime import datetime
import time
app = Flask(__name__)

celery = Celery(
    app.import_name,
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=False
)


DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'celery_task_manager',
    'port': 3306
}

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def init_db():
    try:
       
        conn = pymysql.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )
        cursor = conn.cursor()
        
      
        cursor.execute("CREATE DATABASE IF NOT EXISTS celery_task_manager")
        cursor.execute("USE celery_task_manager")
        
      
        cursor.execute('''CREATE TABLE IF NOT EXISTS tasks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            status VARCHAR(50) DEFAULT 'pending',
            created_at DATETIME,
            processed_at DATETIME NULL
        )''')
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Database started successfully!")
    except Exception as e:
        print(f"Error starting database: {e}")

init_db()


@celery.task
def process_pending_tasks():
  
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
       
        cursor.execute("SELECT id, title FROM tasks WHERE status = 'pending' LIMIT 5")
        pending_tasks = cursor.fetchall()
        
        if pending_tasks:
            print(f"\n[{datetime.now()}] Found {len(pending_tasks)} pending tasks")
            
            for task_id, title in pending_tasks:
                print(f"Processing tasks {task_id}: {title}")
                
                
                time.sleep(2)
                
              
                cursor.execute("""UPDATE tasks 
                                SET status = 'completed', 
                                    processed_at = %s 
                                WHERE id = %s""",
                             (datetime.now(), task_id))
                conn.commit()
                
                print(f"Task {task_id} completed!")
        else:
            print(f"[{datetime.now()}] No pending tasks found")
        
        cursor.close()
        conn.close()
        return f"Processed {len(pending_tasks)} tasks"
    except Exception as e:
        print(f"Error processing tasks: {e}")
        return f"Error: {e}"
    

from celery.schedules import crontab

celery.conf.beat_schedule = {
    'process-pending-tasks-every-10-seconds': {
        'task': 'app.process_pending_tasks',
        'schedule': 10.0,
    }
}



@app.route('/')
def index():
   return jsonify('Hello Priyanshu your Server Started Successfully')


@app.route('/add-task', methods=['POST'])
def add_task():
    try:
        title = request.json.get('title')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO tasks (title, created_at) VALUES (%s, %s)",
                     (title, datetime.now()))
        conn.commit()
        task_id = cursor.lastrowid
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'task_id': task_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
    
@app.route('/tasks')
def get_tasks():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tasks ORDER BY id DESC")
        tasks = cursor.fetchall()
        cursor.close()
        conn.close()
        
        task_list = []
        for task in tasks:
            task_list.append({
                'id': task[0],
                'title': task[1],
                'status': task[2],
                'created_at': str(task[3]) if task[3] else None,
                'processed_at': str(task[4]) if task[4] else None
            })
        
        return jsonify(task_list)
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, port=5000)