import json
import logging
import urllib.request

logger = logging.getLogger(__name__)


def send_slack_alert(webhook_url: str, message: str, level: str = "warning") -> None:
    icon = "🔴" if level == "critical" else "🟡"
    payload = {"text": f"{icon} *DataOps Alert*\n{message}"}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req)
    logger.info("Slack alert sent: %s", message)
