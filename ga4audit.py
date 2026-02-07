from google.oauth2 import service_account
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, Dimension, Metric

import os, json, re, datetime
from dotenv import load_dotenv
from collections import Counter
import requests
import google.auth.transport.requests

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/analytics.edit"
]

SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
if not SERVICE_ACCOUNT_JSON:
    raise Exception("SERVICE_ACCOUNT_JSON environment variable not set")

info = json.loads(SERVICE_ACCOUNT_JSON)
creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

API_BASE = "https://analyticsadmin.googleapis.com/v1beta"

def run_ga4_audit(property_numeric_id, start_date="30daysAgo", end_date="today"):

    admin_client = AnalyticsAdminServiceClient(credentials=creds)
    data_client = BetaAnalyticsDataClient(credentials=creds)

    property_id = f"properties/{property_numeric_id}"
    audit_rows = []

    duplicate_tx_rows = []
    purchase_log = []
    item_error_rows = []

    def log(cat, check, result):
        audit_rows.append({"Category": cat, "Check": check, "Result": result})

    # =========================================================
    # PROPERTY SETTINGS
    # =========================================================
    prop = admin_client.get_property(name=property_id)
    log("Settings", "Display Name", prop.display_name)
    log("Settings", "Time Zone", prop.time_zone)
    log("Settings", "Currency", prop.currency_code)

    # =========================================================
    # DATA RETENTION
    # =========================================================
    try:
        retention = admin_client.get_data_retention_settings(
            name=f"{property_id}/dataRetentionSettings"
        )
        period = retention.event_data_retention.name.replace("_", " ").title()
        log("Settings", "Retention Period", period)
    except Exception as e:
        log("Settings", "Retention Period", str(e))

    # =========================================================
    # ACKNOWLEDGE USER DATA COLLECTION
    # =========================================================
    try:
        if not creds.valid:
            creds.refresh(google.auth.transport.requests.Request())

        token = creds.token
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        url = f"{API_BASE}/{property_id}:acknowledgeUserDataCollection"
        body = {
            "acknowledgement": "I acknowledge that I have the necessary privacy disclosures and rights."
        }

        res = requests.post(url, headers=headers, data=json.dumps(body))
        if res.status_code == 200:
            log("Settings", "User Data Collection", "✅ Acknowledged")
        else:
            log("Settings", "User Data Collection", res.text)
    except Exception as e:
        log("Settings", "User Data Collection", str(e))

    # =========================================================
    # STREAMS
    # =========================================================
    for s in admin_client.list_data_streams(parent=property_id):
        typ = "Web" if s.web_stream_data else "App"
        log("Streams", s.display_name, typ)

    # =========================================================
    # LIMITS
    # =========================================================
    log("Limits", "Custom Dimensions", len(list(admin_client.list_custom_dimensions(parent=property_id))))
    log("Limits", "Custom Metrics", len(list(admin_client.list_custom_metrics(parent=property_id))))
    log("Limits", "Key Events", len(list(admin_client.list_conversion_events(parent=property_id))))
    log("Limits", "Audiences", len(list(admin_client.list_audiences(parent=property_id))))

    # =========================================================
    # EVENT INVENTORY
    # =========================================================
    req = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
    )

    for r in data_client.run_report(request=req).rows:
        log("Events", r.dimension_values[0].value, r.metric_values[0].value)

    # =========================================================
    # PII CHECK
    # =========================================================
    for dim in ["pageLocation"]:
        req = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name=dim)],
            metrics=[Metric(name="eventCount")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
        )
        for r in data_client.run_report(request=req).rows:
            v = r.dimension_values[0].value
            if re.search(r"gmail|phone|@", v):
                log("PII", "Potential PII", v)

    # =========================================================
    # TRANSACTION CHECK
    # =========================================================
    tx_counts = Counter()
    tx_ids = set()

    tx_req = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="transactionId")],
        metrics=[Metric(name="transactions"), Metric(name="purchaseRevenue")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
    )

    for r in data_client.run_report(request=tx_req).rows:
        tid = r.dimension_values[0].value
        count = int(r.metric_values[0].value)
        revenue = r.metric_values[1].value

        if tid:
            tx_ids.add(tid)
            tx_counts[tid] += count

            purchase_log.append({
                "transactionId": tid,
                "revenue": revenue,
                "source": "Revenue"
            })

            if count > 1:
                duplicate_tx_rows.append({"transactionId": tid, "count": count})

    log("Transactions", "Total Transactions", len(tx_ids))
    log("Transactions", "Duplicate Transaction Count", len(duplicate_tx_rows))

    # =========================================================
    # ITEM CHECK
    # =========================================================
    item_req = RunReportRequest(
        property=property_id,
        dimensions=[
            Dimension(name="eventName"),
            Dimension(name="transactionId"),
            Dimension(name="itemName"),
        ],
        metrics=[Metric(name="itemRevenue")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
    )

    item_tx_ids = set()

    for r in data_client.run_report(request=item_req).rows:
        event = r.dimension_values[0].value
        tid = r.dimension_values[1].value
        item = r.dimension_values[2].value
        rev = float(r.metric_values[0].value)

        if event == "purchase":
            purchase_log.append({
                "transactionId": tid,
                "itemName": item,
                "revenue": rev,
                "source": "Item"
            })

            item_tx_ids.add(tid)

            if (item == "(not set)" or item == "") and rev > 0:
                item_error_rows.append({
                    "transactionId": tid,
                    "revenue": rev
                })

    revenue_only = list(tx_ids - item_tx_ids)
    items_only = list(item_tx_ids - tx_ids)

    return {
        "Property Details": [r for r in audit_rows if r["Category"] == "Settings"],
        "Streams": [r for r in audit_rows if r["Category"] == "Streams"],
        "Limits": [r for r in audit_rows if r["Category"] == "Limits"],
        "Events": [r for r in audit_rows if r["Category"] == "Events"],
        "Transactions": [r for r in audit_rows if r["Category"] == "Transactions"],
        "Duplicate Transactions": duplicate_tx_rows,
        "Duplicate Transaction Count": len(duplicate_tx_rows),
        "Transaction Mapping": purchase_log,
        "Item Errors": item_error_rows,
        "Revenue Only": revenue_only,
        "Items Only": items_only
}