# ETL Data Transfer Dashboard

A modern web-based dashboard for managing ETL (Extract, Transform, Load) processes with real-time monitoring and job management capabilities.

## 🚀 Quick Start

### Method 1: Using Batch File (Recommended)

1. **Double-click** `start_server.bat` to start the server
2. **Open browser** and go to `http://127.0.0.1:5000`

### Method 2: Manual Start

1. **Install Dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

2. **Start the Server**:

   ```bash
   python flask_etl_server.py
   ```

3. **Open Dashboard**:
   - Navigate to `http://127.0.0.1:5000` in your browser

## ⚠️ Important Notes

- **DO NOT** open `web_etl.html` directly from file system
- **ALWAYS** use the Flask server at `http://127.0.0.1:5000`
- The application will show warnings if opened incorrectly

## ✨ Features

- **🎯 Real-time ETL Monitoring**: Track job progress with live updates
- **🔄 Multi-Source Data Transfer**: Support for multiple ERP systems (OLD, KN, PR, HH, SCI)
- **📊 Job History & Analytics**: Complete audit trail and performance metrics
- **📱 Responsive Design**: Works perfectly on desktop, tablet, and mobile
- **🌙 Dark/Light Theme**: Toggle between themes for better user experience
- **🔔 Smart Notifications**: Desktop and in-app notifications for job status
- **⚡ Advanced UI**: Skeleton loading, progress indicators, and smooth animations

## 🎮 Usage Guide

### Starting an ETL Job

1. **Navigate** to **ETL Configuration** tab
2. **Fill in** the required fields:
   - **Source Table**: Name of the source table
   - **Target Table**: Name of the target table
   - **ERP Sources**: Select which ERP systems to transfer from
   - **Filter Conditions**: Optional WHERE clause for data filtering
3. **Click** **Start ETL Job**

### Monitoring Jobs

- **📈 Dashboard**: Overview of system status and recent activity
- **🔍 Monitoring**: Real-time view of active jobs with progress tracking
- **📋 History**: Complete log of all completed jobs with detailed logs

### Configuration Options

- **Batch Size**: Number of records to process in each batch (1000-10000)
- **Timeout**: Maximum execution time in minutes (5-120)
- **Safety Options**: Backup before transfer, data validation, notifications

## 🔧 API Endpoints

- `POST /api/etl/start` - Start a new ETL job
- `GET /api/etl/status/<process_id>` - Get job status and progress
- `GET /api/etl/history` - Get job history
- `GET /api/health` - Health check

## 📁 File Structure

```
├── flask_etl_server.py    # Main Flask application
├── web_etl.html          # Dashboard HTML template
├── web_etl.css           # Styling and responsive design
├── web_etl.js            # Frontend JavaScript logic
├── connect_database.py   # Database connection utilities
├── insert_data.py       # ETL data processing functions
├── start_server.bat      # Quick start batch file
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## 🛠️ Troubleshooting

### Common Issues

1. **❌ Port 5000 already in use**:

   - Change the port in `flask_etl_server.py`
   - Or kill the process using port 5000

2. **❌ Database connection errors**:

   - Check database credentials in `connect_database.py`
   - Ensure database servers are running

3. **❌ CORS errors**:

   - Make sure Flask-CORS is installed
   - Check browser console for specific error messages

4. **❌ 405 Method Not Allowed**:
   - **Solution**: Always use Flask server, not direct HTML file
   - Use `http://127.0.0.1:5000` instead of opening HTML directly

### Performance Tips

- Use appropriate batch sizes for your data volume
- Monitor system resources during large transfers
- Consider running ETL jobs during off-peak hours

## 🎨 UI Improvements

The dashboard now includes:

- **Smart Notifications**: Toast messages with action buttons
- **Progress Tracking**: Real-time progress bars and status indicators
- **Skeleton Loading**: Smooth loading states for better UX
- **Responsive Design**: Perfect on all screen sizes
- **Dark Mode**: Beautiful dark theme with consistent colors
- **Keyboard Shortcuts**: Ctrl+K for help, Esc to close modals

## 📞 Support

For issues or questions:

1. Check the browser console for error messages
2. Ensure all dependencies are properly installed
3. Make sure Flask server is running on port 5000
4. Use the built-in help system in the dashboard
