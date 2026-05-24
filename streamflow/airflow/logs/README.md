# Airflow Task Logs

This folder is auto-populated by Airflow at runtime.

## Structure (auto-created by Airflow)

```
logs/
├── dag_id=streamflow_daily_cleanup/
│   └── run_id=scheduled__2025-01-01T00:00:00+00:00/
│       ├── task_id=check_db_connection/
│       │   └── attempt=1.log
│       ├── task_id=count_old_records/
│       │   └── attempt=1.log
│       ├── task_id=delete_old_records/
│       │   └── attempt=1.log
│       ├── task_id=log_database_stats/
│       │   └── attempt=1.log
│       └── task_id=vacuum_table/
│           └── attempt=1.log
└── scheduler/
    └── latest -> symlink to most recent scheduler log folder
```

## Viewing logs

**In the Airflow UI (recommended):**
1. Go to http://localhost:8080
2. Click the `streamflow_daily_cleanup` DAG
3. Click any task → click the log icon

**From the terminal:**
```bash
# Follow scheduler logs live
docker logs -f airflow-scheduler

# View a specific task log
cat logs/dag_id=streamflow_daily_cleanup/run_id=.../task_id=delete_old_records/attempt=1.log
```

## Note

This folder is intentionally empty in the repository.
Logs are written here by the running Airflow containers and are
excluded from version control via `.gitignore`.
