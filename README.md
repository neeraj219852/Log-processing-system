# Distributed Log Processing System

A Python-based distributed log processing system built with PySpark and Streamlit. This system processes log files at scale, performs analytics, generates reports, and provides an interactive dashboard for visualization.

## 🎯 Features

- **Distributed Processing**: Uses PySpark for scalable log processing
- **User-Centric Workflow**: **Drag & Drop CSV Upload** directly in the dashboard
- **Multiple File Support**: Analyze logs from multiple sources simultaneously
- **Analysis History**: Persisted history of past analyses with instant reload capability
- **Comprehensive Analytics**: Error analysis, trends, IP/service breakdowns
- **Alert System**: Configurable alerts based on thresholds
- **Interactive Dashboard**: Modern Streamlit-based visualization
- **Automated Reporting**: CSV and JSON report generation

## 📂 Project Structure

```
distributed-log-system/
│
├── data/
│   ├── history/                  # Persisted analysis files (Parquet)
│   ├── users.db                  # User credentials & History DB
│   └── raw_logs/                 # (Legacy/Backend) Input logs
│
├── reports/
│   ├── csv/                      # CSV reports
│   ├── json/                     # JSON reports
│
├── src/
│   ├── spark/
│   │   ├── ...                   # Backend Analytics Modules
│   │
│   ├── dashboard/
│   │   ├── app.py                # Streamlit dashboard
│   │   ├── history_manager.py    # Analysis history logic
│   │   └── controllers/data_loader.py
│   │
│   └── main.py                   # (Optional) Backend pipeline
│
├── config/
├── requirements.txt
└── README.md
```

## 🚀 Setup

### Prerequisites

- Python 3.8 or higher
- Java 8 or higher (required for PySpark)

### Installation

1. **Clone or navigate to the project directory**

2. **Create a virtual environment (recommended)**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify Java installation**
   ```bash
   java -version
   ```

## 📊 Input Data Format

Place your log CSV files in the `data/raw_logs/` directory. The CSV files should contain at least the following columns:

- `timestamp`: Timestamp of the log entry (various formats supported)
- `log_level`: Log level (INFO, WARN, ERROR, DEBUG)
- `message`: Log message content

Optional columns:
- `ip` or `IP`: IP address
- `service`: Service name
- `endpoint`: Endpoint path

### Example CSV Format

```csv
timestamp,log_level,message,ip,service
2024-01-15 10:30:00,INFO,Request processed successfully,192.168.1.100,api-service
2024-01-15 10:31:00,ERROR,Connection timeout exception,192.168.1.101,api-service
2024-01-15 10:32:00,WARN,High response time detected,192.168.1.102,web-service
```

## 🔧 Configuration

Edit `config/config.yaml` to customize:

- **Spark settings**: Memory, executor settings
- **Paths**: Input/output directories
- **Alert thresholds**: Error rate, error count, critical errors
- **Analytics**: Top N errors, time windows
- **Dashboard**: Auto-refresh settings

## 🏃 Running the System

### 1. Launch Dashboard

Start the interactive dashboard:

```bash
streamlit run src/dashboard/app.py
```

The dashboard will open in your browser at `http://localhost:8501`.

### 2. Upload Data
- Drag and drop one or **multiple CSV files** into the upload area.
- Click **"Analyse"** to process the logs.
- The system supports Linux Syslogs, Spark Logs, and custom CSV formats.

### 3. View Analytics & History
- Explore visual metrics and error breakdowns.
- Use the **Analysis History** button in the sidebar to view and reload past sessions.
- Alert definitions are checked automatically during analysis.

### (Optional) 4. Backend Processing
You can still run the legacy backend pipeline for batch processing of `data/raw_logs/`:

```bash
python src/main.py
```

## 📈 Dashboard Features

The interactive dashboard provides:

- **Key Metrics**: Total logs, errors, error rate, active alerts
- **Charts**:
  - Errors over time (line chart)
  - Top error types (bar chart)
  - Errors per IP (bar chart)
  - Errors per service (bar chart)
- **Filters**: Date range, log level selection
- **Alert Panel**: Recent alerts with timestamps
- **Detailed Tables**: Top errors, trends, severity breakdown
- **Auto-refresh**: Automatic data refresh capability

## 📋 Generated Reports

After processing, reports are available in:

- **CSV Reports** (`reports/csv/`):
  - `errors_by_type.csv`: Errors grouped by type
  - `errors_by_hour.csv`: Errors by hour
  - `top_errors.csv`: Top N most frequent errors
  - `errors_per_ip.csv`: Errors grouped by IP
  - `errors_per_service.csv`: Errors grouped by service

- **JSON Reports** (`reports/json/`):
  - `error_trends.json`: Error trends over time
  - `errors_by_day.json`: Errors by day
  - `errors_by_severity.json`: Errors by severity level

- **Alert Log** (`reports/alerts.log`): History of all alerts

## 🔔 Alert System

The system automatically checks for:

1. **High Error Rate**: Exceeds configured threshold (default: 10%)
2. **High Error Count**: Total errors exceed threshold (default: 100)
3. **Critical Errors**: Critical error count exceeds threshold (default: 5)

Alerts are:
- Printed to console
- Logged to `reports/alerts.log`
- Displayed in the dashboard

## 🛠️ Development

### Code Structure

- **Modular Design**: Each module has a specific responsibility
- **PySpark Only**: All data processing uses PySpark (no Pandas for transformations)
- **Configurable**: Settings in YAML configuration
- **Logging**: Comprehensive logging throughout
- **Error Handling**: Robust error handling and validation

### Adding New Analytics

1. Add function to `src/spark/analytics.py`
2. Call it in `run_all_analytics()`
3. Export results in `export_reports.py`
4. Visualize in dashboard `app.py`

### Customizing Alerts

Edit `config/config.yaml` to adjust:
- `alerts.error_rate_threshold`
- `alerts.error_count_threshold`
- `alerts.critical_error_count`

## 🚀 Deployment

### Local Execution

Currently configured for `local[*]` execution. To run on a cluster:

1. Update `config/config.yaml`:
   ```yaml
   spark:
     master: "spark://your-cluster:7077"
   ```

2. Ensure Spark cluster is accessible
3. Run the same commands

### Cloud Deployment

The code is structured to be cloud-ready. For AWS EMR, Databricks, or other platforms:

1. Package the code
2. Update Spark master URL
3. Ensure data paths are accessible (S3, HDFS, etc.)
4. Deploy and run

## 📝 Notes

- **No Pandas for Processing**: All analytics use PySpark DataFrames
- **Caching**: Intermediate results are cached for performance
- **Partitioning**: Spark optimizations are applied automatically
- **Scalability**: Designed to handle large-scale log processing

## 🐛 Troubleshooting

### Java Not Found
- Install Java 8+ and set `JAVA_HOME` environment variable

### Spark Session Errors
- Check Java installation
- Verify Spark dependencies are installed
- Check configuration file paths

### Dashboard Not Loading Data
- Ensure Spark processing has been run first
- Check that reports are generated in `reports/` directory
- Verify file paths in configuration

### Import Errors
- Ensure you're running from the project root
- Check that all dependencies are installed
- Verify Python path includes project directory

## 📄 License

This project is provided as-is for educational and development purposes.

## 👥 Contributing

Feel free to extend this system with:
- Additional analytics
- New visualization types
- Enhanced alerting
- Performance optimizations

---

**Built with PySpark & Streamlit** 🚀

