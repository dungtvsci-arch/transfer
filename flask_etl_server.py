from flask import Flask, request, jsonify, render_template_string, send_from_directory
try:
    from flask_cors import CORS
except ImportError:
    print("Flask-CORS not found. Installing...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "Flask-CORS"])
    from flask_cors import CORS
import json
import threading
import time
from datetime import datetime
import os
import sys

# Import các module ETL hiện có
from insert_data import (
    connect_db_dev, connect_db_erp_kn, connect_db_erp_pr, 
    connect_db_erp_hh, connect_db_erp_old, connect_db_erp_sci,
    copy_kn_data_to_dev, copy_pr_data_to_dev, copy_hh_data_to_dev,
    copy_old_data_to_dev, copy_sci_data_to_dev, truncate_target_table_dev,
    check_target_table_structure, check_and_create_partitions
)

app = Flask(__name__)
CORS(app)

# Store cho ETL processes
etl_processes = {}

# Khởi tạo bảng 1 lần an toàn luồng cho Flask 3 (thay thế before_first_request)
_init_lock = threading.Lock()
_initialized = False

@app.before_request
def _ensure_initialized_once():
    global _initialized
    if not _initialized:
        with _init_lock:
            if not _initialized:
                try:
                    created = create_etl_processes_table()
                    if created:
                        print("✓ etl_processes ready (first request)")
                    else:
                        print("⚠️ Không thể đảm bảo bảng etl_processes sẵn sàng")
                except Exception as e:
                    print(f"Lỗi init lần đầu: {e}")
                finally:
                    _initialized = True

def create_etl_processes_table():
    """Tạo bảng etl_processes trong DEV database để lưu lịch sử"""
    try:
        dev_conn = connect_db_dev()
        if not dev_conn:
            return False
        
        with dev_conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_processes (
                    process_id VARCHAR(50) PRIMARY KEY,
                    status VARCHAR(20) NOT NULL,
                    progress INTEGER DEFAULT 0,
                    current_step TEXT,
                    total_records INTEGER DEFAULT 0,
                    error TEXT,
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    source_table VARCHAR(100),
                    target_table VARCHAR(100),
                    sources TEXT,
                    where_clause TEXT,
                    batch_size INTEGER DEFAULT 5000,
                    results JSONB
                )
            """)
            dev_conn.commit()
            print("✓ Bảng etl_processes đã được tạo/cập nhật")
            return True
    except Exception as e:
        print(f"Lỗi khi tạo bảng etl_processes: {e}")
        return False
    finally:
        if dev_conn:
            dev_conn.close()

def save_etl_process_to_db(process):
    """Lưu thông tin ETL process vào database"""
    try:
        dev_conn = connect_db_dev()
        if not dev_conn:
            return False
        
        with dev_conn.cursor() as cursor:
            # Kiểm tra xem process đã tồn tại chưa
            cursor.execute("SELECT process_id FROM etl_processes WHERE process_id = %s", (process.process_id,))
            exists = cursor.fetchone()
            
            if exists:
                # Cập nhật
                cursor.execute("""
                    UPDATE etl_processes SET 
                        status = %s,
                        progress = %s,
                        current_step = %s,
                        total_records = %s,
                        error = %s,
                        end_time = %s,
                        results = %s
                    WHERE process_id = %s
                """, (
                    process.status,
                    process.progress,
                    process.current_step,
                    process.total_records,
                    process.error,
                    process.end_time,
                    json.dumps(process.results) if process.results else None,
                    process.process_id
                ))
            else:
                # Thêm mới
                cursor.execute("""
                    INSERT INTO etl_processes 
                    (process_id, status, progress, current_step, total_records, error, 
                     start_time, end_time, results)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    process.process_id,
                    process.status,
                    process.progress,
                    process.current_step,
                    process.total_records,
                    process.error,
                    process.start_time,
                    process.end_time,
                    json.dumps(process.results) if process.results else None
                ))
            
            dev_conn.commit()
            return True
    except Exception as e:
        print(f"Lỗi khi lưu ETL process: {e}")
        return False
    finally:
        if dev_conn:
            dev_conn.close()

class ETLProcess:
    def __init__(self, process_id):
        self.process_id = process_id
        self.status = "pending"
        self.progress = 0
        self.current_step = ""
        self.logs = []
        self.results = {}
        self.total_records = 0
        self.error = None
        self.start_time = None
        self.end_time = None

    def add_log(self, message, level="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {
            "timestamp": timestamp,
            "message": message,
            "level": level
        }
        self.logs.append(log_entry)
        print(f"[{timestamp}] {level.upper()}: {message}")

    def update_progress(self, progress, step):
        self.progress = progress
        self.current_step = step

@app.route('/')
def index():
    """Serve the main ETL web interface"""
    try:
        # Get the directory of the current script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        html_file = os.path.join(current_dir, 'web_etl.html')
        
        print(f"Looking for HTML file in: {current_dir}")
        print(f"HTML file: {html_file} - Exists: {os.path.exists(html_file)}")
        
        # Simply serve the HTML file without injection
        return send_from_directory(current_dir, 'web_etl.html')
        
    except FileNotFoundError as e:
        error_msg = f"""
        <html>
        <head><title>File Not Found</title></head>
        <body>
            <h1>Error: File Not Found</h1>
            <p>Could not find required files: {e}</p>
            <p>Current directory: {os.path.dirname(os.path.abspath(__file__))}</p>
            <p>Please ensure all files are in the same directory as flask_etl_server.py</p>
        </body>
        </html>
        """
        return error_msg, 404
    except Exception as e:
        error_msg = f"""
        <html>
        <head><title>Server Error</title></head>
        <body>
            <h1>Error: {str(e)}</h1>
            <p>Please check the server logs for more details.</p>
        </body>
        </html>
        """
        return error_msg, 500

@app.route('/web_etl.css')
def serve_css():
    """Serve CSS file"""
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), 'web_etl.css')

@app.route('/web_etl.js')
def serve_js():
    """Serve JS file"""
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), 'web_etl.js')

@app.route('/api/etl/start', methods=['POST'])
def start_etl():
    """Start ETL process"""
    try:
        data = request.get_json()
        
        # Validate input
        if not data.get('sourceTable'):
            return jsonify({"success": False, "message": "Source table is required"}), 400
        
        if not data.get('targetTable'):
            return jsonify({"success": False, "message": "Target table is required"}), 400
        
        if not data.get('sources'):
            return jsonify({"success": False, "message": "At least one source must be selected"}), 400
        
        # Create process ID
        process_id = f"etl_{int(time.time())}"
        
        # Create ETL process
        etl_process = ETLProcess(process_id)
        etl_process.start_time = datetime.now()
        etl_processes[process_id] = etl_process
        
        # Start ETL in background thread
        thread = threading.Thread(target=run_etl_process, args=(process_id, data))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            "success": True,
            "process_id": process_id,
            "message": "ETL process started",
            "data": {
                "process_id": process_id
            }
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/etl/status/<process_id>')
def get_etl_status(process_id):
    """Get ETL process status"""
    if process_id not in etl_processes:
        return jsonify({"success": False, "message": "Process not found"}), 404
    
    process = etl_processes[process_id]
    
    return jsonify({
        "success": True,
        "status": process.status,
        "progress": process.progress,
        "current_step": process.current_step,
        "logs": process.logs[-10:],  # Last 10 logs
        "results": process.results,
        "total_records": process.total_records,
        "error": process.error,
        "start_time": process.start_time.isoformat() if process.start_time else None,
        "end_time": process.end_time.isoformat() if process.end_time else None
    })

def run_etl_process(process_id, data):
    """Run ETL process in background"""
    process = etl_processes[process_id]
    
    # Lưu thông tin ban đầu vào database
    process.source_table = data.get('sourceTable')
    process.target_table = data.get('targetTable')
    process.sources = ','.join(data.get('sources', []))
    process.where_clause = data.get('whereClause')
    process.batch_size = data.get('batchSize', 5000)
    save_etl_process_to_db(process)
    
    try:
        process.status = "running"
        process.add_log("Bắt đầu quá trình ETL...", "info")
        process.update_progress(5, "Đang kết nối databases...")
        save_etl_process_to_db(process)
        
        # Connect to databases
        dev_conn = connect_db_dev()
        if not dev_conn:
            raise Exception("Không thể kết nối đến DEV database")
        
        process.add_log("Đã kết nối DEV database", "success")
        
        # Check target table structure
        is_partitioned = check_target_table_structure(dev_conn, data['targetTable'])
        if is_partitioned:
            process.add_log("Bảng đích là partitioned table", "info")
            brand_etl_list = []
            for source in data['sources']:
                brand_etl_map = {'old': 0, 'kn': 1, 'pr': 3, 'hh': 4, 'sci': 7}
                brand_etl_list.append(brand_etl_map.get(source))
            check_and_create_partitions(dev_conn, data['targetTable'], brand_etl_list)
        
        process.update_progress(15, "Đang xóa dữ liệu cũ...")
        save_etl_process_to_db(process)
        
        # Truncate target table
        truncate_target_table_dev(dev_conn, data['targetTable'])
        process.add_log(f"Đã xóa dữ liệu trong bảng {data['targetTable']}", "info")
        
        # Connect to source databases
        connections = {}
        source_conn_map = {
            'old': connect_db_erp_old,
            'kn': connect_db_erp_kn,
            'pr': connect_db_erp_pr,
            'hh': connect_db_erp_hh,
            'sci': connect_db_erp_sci
        }
        
        for source in data['sources']:
            if source in source_conn_map:
                conn = source_conn_map[source]()
                if conn:
                    connections[source] = conn
                    process.add_log(f"Đã kết nối {source.upper()} database", "success")
                else:
                    raise Exception(f"Không thể kết nối đến {source.upper()} database")
        
        process.update_progress(25, "Đang copy dữ liệu...")
        save_etl_process_to_db(process)
        
        # Copy data from each source
        total_inserted = 0
        copy_functions = {
            'old': copy_old_data_to_dev,
            'kn': copy_kn_data_to_dev,
            'pr': copy_pr_data_to_dev,
            'hh': copy_hh_data_to_dev,
            'sci': copy_sci_data_to_dev
        }
        
        brand_etl_map = {'old': 0, 'kn': 1, 'pr': 3, 'hh': 4, 'sci': 7}
        
        for i, source in enumerate(data['sources']):
            if source in connections and source in copy_functions:
                progress = 25 + (i + 1) * 60 // len(data['sources'])
                process.update_progress(progress, f"Đang copy từ {source.upper()}...")
                save_etl_process_to_db(process)
                
                try:
                    inserted = copy_functions[source](
                        dev_conn, 
                        connections[source], 
                        data['sourceTable'], 
                        data['targetTable'], 
                        brand_etl=brand_etl_map[source],
                        where_clause=data.get('whereClause'),
                        batch_size=data.get('batchSize', 5000)
                    )
                    
                    process.results[source] = inserted
                    total_inserted += inserted
                    process.add_log(f"Đã chèn {inserted:,} records từ {source.upper()}", "success")
                    
                except Exception as e:
                    process.add_log(f"Lỗi khi copy từ {source.upper()}: {str(e)}", "error")
                    raise
        
        process.total_records = total_inserted
        process.update_progress(100, "Hoàn thành")
        process.status = "completed"
        process.add_log(f"ETL hoàn thành! Tổng cộng {total_inserted:,} records", "success")
        
    except Exception as e:
        process.status = "failed"
        process.error = str(e)
        process.add_log(f"ETL thất bại: {str(e)}", "error")
    
    finally:
        process.end_time = datetime.now()
        save_etl_process_to_db(process)
        
        # Close all connections
        if 'dev_conn' in locals() and dev_conn:
            try:
                dev_conn.close()
            except:
                pass
        
        for conn in connections.values():
            try:
                conn.close()
            except:
                pass

@app.route('/api/etl/history')
def get_etl_history():
    """Get ETL process history from database"""
    try:
        dev_conn = connect_db_dev()
        if not dev_conn:
            return jsonify({"success": False, "message": "Không thể kết nối đến DEV database"}), 500
        
        with dev_conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    process_id,
                    status,
                    progress,
                    current_step,
                    total_records,
                    error,
                    start_time,
                    end_time,
                    source_table,
                    target_table,
                    sources,
                    where_clause,
                    batch_size,
                    results
                FROM etl_processes
                ORDER BY start_time DESC
                LIMIT 50
            """)
            
            history = []
            for row in cursor.fetchall():
                # Handle JSON parsing safely
                results_data = {}
                if row[13]:  # results field
                    try:
                        if isinstance(row[13], str):
                            results_data = json.loads(row[13])
                        elif isinstance(row[13], dict):
                            results_data = row[13]
                    except (json.JSONDecodeError, TypeError):
                        results_data = {}
                
                history.append({
                    "process_id": row[0],
                    "status": row[1],
                    "progress": row[2],
                    "current_step": row[3],
                    "total_records": row[4],
                    "error": row[5],
                    "start_time": row[6].isoformat() if row[6] else None,
                    "end_time": row[7].isoformat() if row[7] else None,
                    "source_table": row[8],
                    "target_table": row[9],
                    "sources": row[10],
                    "where_clause": row[11],
                    "batch_size": row[12],
                    "results": results_data
                })
        
        dev_conn.close()
        return jsonify({"success": True, "history": history})
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_processes": len([p for p in etl_processes.values() if p.status == "running"])
    })

@app.route('/api/stats/dashboard')
def get_dashboard_stats():
    """Get real dashboard statistics from DEV database"""
    try:
        dev_conn = connect_db_dev()
        if not dev_conn:
            return jsonify({"success": False, "message": "Không thể kết nối đến DEV database"}), 500
        
        stats = {}
        
        with dev_conn.cursor() as cursor:
            # Check which tables exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('ir_model_data_new', 'product_product_new', 'res_brand_new', 'product_template_new')
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
            
            if not existing_tables:
                # No tables exist yet, return empty stats
                stats = {
                    'total_records': 0,
                    'total_brands': 0,
                    'total_tables': 0,
                    'brand_stats': [],
                    'table_stats': [],
                    'recent_activity': {
                        'total_jobs': 0,
                        'completed_jobs': 0,
                        'failed_jobs': 0,
                        'running_jobs': 0,
                        'total_processed_records': 0
                    }
                }
            else:
                # Build dynamic query based on existing tables
                union_parts = []
                for table in existing_tables:
                    union_parts.append(f"SELECT '{table.replace('_new', '')}' as table_name, brand_etl FROM {table}")
                
                union_query = " UNION ALL ".join(union_parts)
                
                # Get total records count
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT brand_etl) as total_brands,
                        COUNT(DISTINCT table_name) as total_tables
                    FROM ({union_query}) t
                """)
                result = cursor.fetchone()
                stats['total_records'] = result[0] if result[0] else 0
                stats['total_brands'] = result[1] if result[1] else 0
                stats['total_tables'] = result[2] if result[2] else 0
                
                # Get records by brand_etl
                cursor.execute(f"""
                    SELECT 
                        brand_etl,
                        CASE 
                            WHEN brand_etl = 0 THEN 'OLD ERP'
                            WHEN brand_etl = 1 THEN 'KN ERP'
                            WHEN brand_etl = 3 THEN 'PR ERP'
                            WHEN brand_etl = 4 THEN 'HH ERP'
                            WHEN brand_etl = 7 THEN 'SCI ERP'
                            ELSE 'Unknown'
                        END as brand_name,
                        COUNT(*) as record_count
                    FROM ({union_query}) t
                    GROUP BY brand_etl
                    ORDER BY brand_etl
                """)
                brand_stats = []
                for row in cursor.fetchall():
                    brand_stats.append({
                        'brand_etl': row[0],
                        'brand_name': row[1],
                        'record_count': row[2]
                    })
                stats['brand_stats'] = brand_stats
                
                # Get records by table
                table_stats = []
                for table in existing_tables:
                    cursor.execute(f"""
                        SELECT 
                            '{table.replace('_new', '')}' as table_name,
                            COUNT(*) as record_count,
                            COUNT(DISTINCT brand_etl) as brand_count
                        FROM {table}
                    """)
                    row = cursor.fetchone()
                    table_stats.append({
                        'table_name': row[0],
                        'record_count': row[1],
                        'brand_count': row[2]
                    })
                
                # Sort by record count
                table_stats.sort(key=lambda x: x['record_count'], reverse=True)
                stats['table_stats'] = table_stats
            
            # Get recent ETL activity (last 24 hours) - check if etl_processes table exists
            try:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_jobs,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_jobs,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
                        SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running_jobs,
                        COALESCE(SUM(total_records), 0) as total_processed_records
                    FROM etl_processes
                    WHERE start_time >= NOW() - INTERVAL '24 hours'
                """)
                activity_result = cursor.fetchone()
                stats['recent_activity'] = {
                    'total_jobs': activity_result[0] if activity_result[0] else 0,
                    'completed_jobs': activity_result[1] if activity_result[1] else 0,
                    'failed_jobs': activity_result[2] if activity_result[2] else 0,
                    'running_jobs': activity_result[3] if activity_result[3] else 0,
                    'total_processed_records': activity_result[4] if activity_result[4] else 0
                }
            except Exception as e:
                # etl_processes table doesn't exist yet
                stats['recent_activity'] = {
                    'total_jobs': 0,
                    'completed_jobs': 0,
                    'failed_jobs': 0,
                    'running_jobs': 0,
                    'total_processed_records': 0
                }
            
        dev_conn.close()
        
        return jsonify({
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Error in get_dashboard_stats: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/stats/tables')
def get_table_details():
    """Get detailed table information"""
    try:
        dev_conn = connect_db_dev()
        if not dev_conn:
            return jsonify({"success": False, "message": "Không thể kết nối đến DEV database"}), 500
        
        tables_info = []
        table_names = ['ir_model_data_new', 'product_product_new', 'res_brand_new', 'product_template_new']
        
        with dev_conn.cursor() as cursor:
            for table_name in table_names:
                try:
                    # Get table structure
                    cursor.execute("""
                        SELECT 
                            column_name,
                            data_type,
                            is_nullable,
                            column_default
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        ORDER BY ordinal_position
                    """, (table_name,))
                    
                    columns = []
                    for col in cursor.fetchall():
                        columns.append({
                            'name': col[0],
                            'type': col[1],
                            'nullable': col[2] == 'YES',
                            'default': col[3]
                        })
                    
                    # Get record counts by brand
                    cursor.execute(f"""
                        SELECT 
                            brand_etl,
                            CASE 
                                WHEN brand_etl = 0 THEN 'OLD ERP'
                                WHEN brand_etl = 1 THEN 'KN ERP'
                                WHEN brand_etl = 3 THEN 'PR ERP'
                                WHEN brand_etl = 4 THEN 'HH ERP'
                                WHEN brand_etl = 7 THEN 'SCI ERP'
                                ELSE 'Unknown'
                            END as brand_name,
                            COUNT(*) as record_count
                        FROM {table_name}
                        GROUP BY brand_etl
                        ORDER BY brand_etl
                    """)
                    
                    brand_counts = []
                    for row in cursor.fetchall():
                        brand_counts.append({
                            'brand_etl': row[0],
                            'brand_name': row[1],
                            'record_count': row[2]
                        })
                    
                    # Get total count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    total_count = cursor.fetchone()[0]
                    
                    tables_info.append({
                        'table_name': table_name,
                        'total_records': total_count,
                        'columns': columns,
                        'brand_counts': brand_counts,
                        'last_updated': datetime.now().isoformat()
                    })
                    
                except Exception as e:
                    tables_info.append({
                        'table_name': table_name,
                        'error': str(e),
                        'total_records': 0,
                        'columns': [],
                        'brand_counts': []
                    })
        
        dev_conn.close()
        
        return jsonify({
            "success": True,
            "tables": tables_info,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/debug')
def debug_info():
    """Debug information endpoint"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    files = {
        'html': os.path.exists(os.path.join(current_dir, 'web_etl.html')),
        'css': os.path.exists(os.path.join(current_dir, 'web_etl.css')),
        'js': os.path.exists(os.path.join(current_dir, 'web_etl.js')),
        'server': os.path.exists(os.path.join(current_dir, 'flask_etl_server.py'))
    }
    
    return jsonify({
        "current_directory": current_dir,
        "files_exist": files,
        "server_running": True,
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("🚀 Starting ETL Web Server...")
    print("📊 Web Interface: http://localhost:5000", "or", "http://127.0.0.1:5000")
    print("🔧 API Endpoints:")
    print("   - POST /api/etl/start - Start ETL process")
    print("   - GET /api/etl/status/<process_id> - Get process status")
    print("   - GET /api/etl/history - Get ETL history")
    print("   - GET /api/stats/dashboard - Get dashboard statistics")
    print("   - GET /api/stats/tables - Get table details")
    print("   - GET /api/health - Health check")
    print("   - GET /debug - Debug information")
    print()
    print("📁 Checking files...")
    
    # Check if files exist
    current_dir = os.path.dirname(os.path.abspath(__file__))
    files_to_check = ['web_etl.html', 'web_etl.css', 'web_etl.js']
    
    for file in files_to_check:
        file_path = os.path.join(current_dir, file)
        exists = os.path.exists(file_path)
        print(f"   {'✅' if exists else '❌'} {file}: {'Found' if exists else 'NOT FOUND'}")
    
    print()
    print("🗄️ Initializing database...")
    # Tạo bảng etl_processes
    if create_etl_processes_table():
        print("✅ Database initialized successfully")
    else:
        print("❌ Database initialization failed")
    
    print()
    print("🌐 Server starting...")
    print("⚠️  If you see any errors above, please check file locations!")
    print()
    
    port = int(os.environ.get("PORT", 10000))  # Render sẽ inject PORT động
    app.run(debug=False, host='0.0.0.0', port=port)
