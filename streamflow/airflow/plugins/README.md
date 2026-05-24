# Airflow Plugins

Place custom Airflow plugins in this folder.

## What goes here?

Airflow auto-discovers any Python file placed here. You can add:

- **Custom operators** — e.g. a `KafkaPublishOperator` that publishes a
  message to a Kafka topic from inside a DAG
- **Custom sensors** — e.g. a `PostgresRowCountSensor` that waits until
  a table reaches a minimum row count before proceeding
- **Custom hooks** — reusable connection helpers for external services
- **Macros** — Jinja template functions usable inside DAG definitions

## Example: custom Slack alert operator

```python
# streamflow/airflow/plugins/slack_alert_operator.py

from airflow.models import BaseOperator
import requests

class SlackAlertOperator(BaseOperator):
    def __init__(self, webhook_url: str, message: str, **kwargs):
        super().__init__(**kwargs)
        self.webhook_url = webhook_url
        self.message = message

    def execute(self, context):
        requests.post(self.webhook_url, json={"text": self.message})
```

Then use it in a DAG:

```python
from plugins.slack_alert_operator import SlackAlertOperator

alert = SlackAlertOperator(
    task_id="send_slack_alert",
    webhook_url="https://hooks.slack.com/...",
    message="StreamFlow cleanup completed!",
)
```
