"""
Airflow Webserver Configuration
=================================
Minimal config for the StreamFlow Airflow webserver.
Loaded automatically by Airflow on startup.
"""

from __future__ import annotations

# Use database authentication (username + password)
AUTH_TYPE = 1   # AUTH_DB

# Session cookie name
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Secret key — change this in production!
SECRET_KEY = "streamflow-change-me-in-production"
