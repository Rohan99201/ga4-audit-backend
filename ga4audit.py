# ✅ UPDATED CODE: Uses Service Account + Fixes for Duplicate Transactions, Item Errors, and Adds Retention & Acknowledge Check

from google.oauth2 import service_account
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1beta.types import GetDataRetentionSettingsRequest, AcknowledgeUserDataCollectionRequest
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, Dimension, Metric
import pandas as pd
import re
import os
import json
from dotenv import load_dotenv

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
          'https://www.googleapis.com/auth/analytics.edit']

# ✅ Load credentials from JSON stored in environment variable
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
if not SERVICE_ACCOUNT_JSON:
    raise Exception("SERVICE_ACCOUNT_JSON environment variable is not set")

info = json.loads(SERVICE_ACCOUNT_JSON)
creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

def run_ga4_audit(property_numeric_id, start_date="30daysAgo", end_date="today"):
    admin_client = AnalyticsAdminServiceClient(credentials=creds)
    data_client = BetaAnalyticsDataClient(credentials=creds)

    property_id = f'properties/{property_numeric_id}'
    audit_rows = []
    tx_detail_rows = []
    item_error_rows = []
    duplicate_transaction_rows = []
    pii_found = False

    def log(category, check, result):
        audit_rows.append({'Category': category, 'Check': check, 'Result': result})

    # ✅ Basic Settings
    prop = admin_client.get_property(name=property_id)
    log("Settings", "Display Name", prop.display_name)
    log("Settings", "Time Zone", prop.time_zone)
    log("Settings", "Currency", prop.currency_code)
    log("Settings", "Reporting Identity", "Not available via API")

    try:
        retention = admin_client.get_data_retention_settings(request=GetDataRetentionSettingsRequest(name=f"properties/{property_numeric_id}/dataRetentionSettings"))
        log("Settings", "Retention Period (Days)", retention.event_data_retention)
    except Exception as e:
        log("Settings", "Retention Period (Days)", f"Error: {str(e)}")

    try:
        admin_client.acknowledge_user_data_collection(request=AcknowledgeUserDataCollectionRequest(property=property_id))
        log("Settings", "User Data Collection", "✅ Acknowledged")
    except Exception as e:
        log("Settings", "User Data Collection", f"❌ Failed to Acknowledge: {str(e)}")

    # ✅ Streams
    streams = admin_client.list_data_streams(parent=property_id)
    for stream in streams:
        stream_type = "Web" if hasattr(stream, "web_stream_data") and stream.web_stream_data else \
                      "Android" if hasattr(stream, "android_app_stream_data") and stream.android_app_stream_data else \
                      "iOS" if hasattr(stream, "ios_app_stream_data") and stream.ios_app_stream_data else "Unknown"
        stream_name = stream.display_name or "Unnamed Stream"
        log("Streams", f"{stream_name} ({stream_type})", stream.name)

    # ✅ Limits
    custom_dims = list(admin_client.list_custom_dimensions(parent=property_id))
    custom_metrics = list(admin_client.list_custom_metrics(parent=property_id))
    key_events = list(admin_client.list_conversion_events(parent=property_id))
    audiences = list(admin_client.list_audiences(parent=property_id))
    log("Limits", "Custom Dimensions Used", f"{len(custom_dims)} / 50")
    log("Limits", "Custom Metrics Used", f"{len(custom_metrics)} / 50")
    log("Limits", "Key Events Used", f"{len(key_events)} / 50")
    log("Limits", "Audiences Used", f"{len(audiences)} / 100")

    # ✅ Events
    event_inventory_req = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        limit=200
    )
    response = data_client.run_report(request=event_inventory_req)
    event_list = []
    for row in response.rows:
        event = row.dimension_values[0].value
        event_list.append(event)
        log("Event Inventory", event, row.metric_values[0].value)

    # ✅ PII Check
    for dim in ["pagePath", "pageLocation"]:
        try:
            pii_req = RunReportRequest(
                property=property_id,
                dimensions=[Dimension(name=dim)],
                metrics=[Metric(name="eventCount")],
                date_ranges=[{"start_date": start_date, "end_date": end_date}],
                limit=100
            )
            response = data_client.run_report(request=pii_req)
            for row in response.rows:
                val = row.dimension_values[0].value
                if re.search(r"gmail\\.com|email=|phone=|pno=|\\+91\\d{10}|\\d{10}", val):
                    log("PII", f"Potential PII in {dim}", val)
                    pii_found = True
        except Exception as e:
            log("PII", f"{dim} scan failed", str(e))
    if not pii_found:
        log("PII", "Scan Result", "✅ No potential PII found in page paths or URLs.")

    # ✅ Duplicate Transaction Check
    txn_report = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="transactionId")],
        metrics=[Metric(name="transactions"), Metric(name="purchaseRevenue")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        limit=1000
    )
    response = data_client.run_report(request=txn_report)
    seen_txn = {}
    total_txn_ids = set()

    for row in response.rows:
        tid = row.dimension_values[0].value
        transactions = int(row.metric_values[0].value)
        revenue = float(row.metric_values[1].value)
        if tid:
            total_txn_ids.add(tid)
            tx_detail_rows.append({"transactionId": tid, "revenue": revenue, "source": "Revenue Table"})
            if transactions > 1:
                duplicate_transaction_rows.append({"transactionId": tid, "count": transactions})

    log("Transactions", "Total Unique transactionId", len(total_txn_ids))
    if duplicate_transaction_rows:
        log("Transactions", "Duplicate Transaction IDs", str(duplicate_transaction_rows))
    else:
        log("Transactions", "Duplicate Transaction IDs", "✅ No duplicates found")

    # ✅ Item Mapping
    item_report = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="eventName"), Dimension(name="transactionId"), Dimension(name="itemId"), Dimension(name="itemName")],
        metrics=[Metric(name="itemRevenue")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        limit=1000
    )
    response = data_client.run_report(request=item_report)
    item_tx_ids = set()
    for row in response.rows:
        event = row.dimension_values[0].value
        tid = row.dimension_values[1].value
        item_id = row.dimension_values[2].value
        item_name = row.dimension_values[3].value
        item_revenue = float(row.metric_values[0].value)

        if event == "purchase" and tid:
            item_tx_ids.add(tid)
            tx_detail_rows.append({"transactionId": tid, "itemId": item_id, "itemName": item_name, "source": "Item Table"})
            if item_name in ["", "(not set)"] and item_revenue > 0:
                item_error_rows.append({"transactionId": tid, "itemId": item_id, "itemName": item_name, "itemRevenue": item_revenue})

    missing_in_items = total_txn_ids - item_tx_ids
    missing_in_txns = item_tx_ids - total_txn_ids
    if not missing_in_items:
        log("Transactions", "With Revenue but Missing Items", "✅ All revenue transactions are linked to items.")
    else:
        log("Transactions", "With Revenue but Missing Items", str(missing_in_items))
    if not missing_in_txns:
        log("Transactions", "With Items but No Revenue", "✅ All item transactions have matching revenue data.")
    else:
        log("Transactions", "With Items but No Revenue", str(missing_in_txns))

    return {
        "Property Details": [row for row in audit_rows if row['Category'] == "Settings"],
        "Streams Configuration": [row for row in audit_rows if row['Category'] == "Streams"],
        "GA4 Property Limits": [row for row in audit_rows if row['Category'] == "Limits"],
        "GA4 Events": [row for row in audit_rows if row['Category'] == "Event Inventory"],
        "PII Check": [row for row in audit_rows if row['Category'] == "PII"],
        "Transactions": [row for row in audit_rows if row['Category'] == "Transactions"],
        "Transaction Mapping": tx_detail_rows,
        "Errors in Item Data": item_error_rows,
        "Duplicate Transactions": duplicate_transaction_rows
    }
