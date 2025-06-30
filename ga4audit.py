# ✅ UPDATED CODE: Detect Duplicate Transactions, Fix Currency Error, Handle ItemName Null, Add Acknowledge & Retention, Correct TimeZone

from google.oauth2 import service_account
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1beta.types import AcknowledgeUserDataCollectionRequest
from google.analytics.admin_v1beta.types import GetDataRetentionSettingsRequest, DataRetentionSettings # Import DataRetentionSettings for enum
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, Dimension, Metric
import pandas as pd
import re
import os
import json
from dotenv import load_dotenv
from collections import Counter

load_dotenv()

# ✅ UPDATED SCOPES to include analytics.edit for acknowledgeUserDataCollection
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly', 'https://www.googleapis.com/auth/analytics.edit']

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
    duplicate_tx_rows = []
    purchase_log = []
    pii_found = False

    def log(category, check, result):
        audit_rows.append({'Category': category, 'Check': check, 'Result': result})

    prop = admin_client.get_property(name=property_id)
    log("Settings", "Display Name", prop.display_name)
    log("Settings", "Time Zone", prop.time_zone)
    log("Settings", "Currency", prop.currency_code)

    # ✅ Acknowledge user data collection
    acknowledgement_string = "I acknowledge that I have the necessary privacy disclosures and rights from my end users for the collection and processing of their data, including the association of such data with the visitation information Google Analytics collects from my site and/or app property."
    try:
        # Corrected: Pass property_id as the first positional argument, then the request object
        admin_client.acknowledge_user_data_collection(
            property_id, # The resource name for the property as a positional argument
            request=AcknowledgeUserDataCollectionRequest(
                acknowledgement=acknowledgement_string
            )
        )
        log("Settings", "User Data Collection Acknowledgment", "✅ Acknowledged successfully.")
    except Exception as e:
        log("Settings", "User Data Collection Acknowledgment", f"❌ Failed to acknowledge: {e}")

    # ✅ Retention settings
    try:
        retention_settings = admin_client.get_data_retention_settings(
            name=f"properties/{property_numeric_id}/dataRetentionSettings"
        )
        retention_period_str = retention_settings.event_data_retention.name.replace('_', ' ').title()
        log("Settings", "Retention Period", retention_period_str)
    except Exception as e:
        log("Settings", "Retention Period", f"Not available via API ({e})")

    # ✅ Streams
    streams = admin_client.list_data_streams(parent=property_id)
    for stream in streams:
        stream_type = "Web" if stream.web_stream_data else "Android" if stream.android_app_stream_data else "iOS" if stream.ios_app_stream_data else "Unknown"
        stream_name = stream.display_name or "Unnamed Stream"
        log("Streams", f"{stream_name} ({stream_type})", stream.name)

    # ✅ Limits
    log("Limits", "Custom Dimensions Used", f"{len(list(admin_client.list_custom_dimensions(parent=property_id)))} / 50")
    log("Limits", "Custom Metrics Used", f"{len(list(admin_client.list_custom_metrics(parent=property_id)))} / 50")
    log("Limits", "Key Events Used", f"{len(list(admin_client.list_conversion_events(parent=property_id)))} / 50")
    log("Limits", "Audiences Used", f"{len(list(admin_client.list_audiences(parent=property_id)))} / 100")

    # ✅ Event Inventory
    inventory_req = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}]
    )
    for row in data_client.run_report(request=inventory_req).rows:
        log("Event Inventory", row.dimension_values[0].value, row.metric_values[0].value)

    # ✅ PII Check
    for dim in ["pagePath", "pageLocation"]:
        try:
            pii_req = RunReportRequest(
                property=property_id,
                dimensions=[Dimension(name=dim)],
                metrics=[Metric(name="eventCount")],
                date_ranges=[{"start_date": start_date, "end_date": end_date}]
            )
            for row in data_client.run_report(request=pii_req).rows:
                val = row.dimension_values[0].value
                if re.search(r"gmail\\.com|email=|phone=|pno=|\\+91\\d{10}|\\d{10}", val):
                    log("PII", f"Potential PII in {dim}", val)
                    pii_found = True
        except Exception:
            continue
    if not pii_found:
        log("PII", "Scan Result", "✅ No potential PII found in page paths or URLs.")

    # ✅ Transaction-level check with duplicate detection
    transaction_ids = set()
    transaction_counts = Counter()
    tx_report = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="transactionId")],
        metrics=[Metric(name="transactions"), Metric(name="purchaseRevenue")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}]
    )
    for row in data_client.run_report(request=tx_report).rows:
        tid = row.dimension_values[0].value
        count = int(row.metric_values[0].value)
        revenue = row.metric_values[1].value
        if tid:
            transaction_ids.add(tid)
            transaction_counts[tid] += count
            tx_detail_rows.append({"transactionId": tid, "revenue": revenue, "source": "Revenue Table"})
            if count > 1:
                duplicate_tx_rows.append({"transactionId": tid, "count": count})
    log("Transactions", "Total Unique transactionId", len(transaction_ids))
    log("Transactions", "Duplicate Transaction Count", len(duplicate_tx_rows))
    log("Transactions", "Duplicate Transaction IDs", duplicate_tx_rows or "✅ No duplicates found")

    # ✅ Item-level check
    item_transaction_ids = set()
    try:
        item_report = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="eventName"), Dimension(name="transactionId"), Dimension(name="itemId"), Dimension(name="itemName")],
            metrics=[Metric(name="itemRevenue")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}]
        )
        for row in data_client.run_report(request=item_report).rows:
            event_name = row.dimension_values[0].value
            tid = row.dimension_values[1].value
            item_id = row.dimension_values[2].value
            item_name = row.dimension_values[3].value
            revenue = row.metric_values[0].value
            if event_name == "purchase":
                tx_detail_rows.append({"transactionId": tid, "itemId": item_id, "itemName": item_name, "revenue": revenue, "source": "Item Table"})
                if tid:
                    item_transaction_ids.add(tid)
                    if item_name in ["", "(not set)"] and float(revenue) > 0:
                        item_error_rows.append({"transactionId": tid, "itemId": item_id, "itemName": item_name, "revenue": revenue})
    except Exception as e:
        log("Transactions", "Item-level check failed", str(e))

    # ✅ Compare mapping
    missing_in_items = transaction_ids - item_transaction_ids
    missing_in_txns = item_transaction_ids - transaction_ids
    log("Transactions", "With Revenue but Missing Items", "✅ All revenue transactions are linked to items." if not missing_in_items else str(missing_in_items))
    log("Transactions", "With Items but No Revenue", "✅ All item transactions have matching revenue data." if not missing_in_txns else str(missing_in_txns))

    return {
        "Property Details": [r for r in audit_rows if r['Category'] == "Settings"],
        "Streams Configuration": [r for r in audit_rows if r['Category'] == "Streams"],
        "GA4 Property Limits": [r for r in audit_rows if r['Category'] == "Limits"],
        "GA4 Events": [r for r in audit_rows if r['Category'] == "Event Inventory"],
        "PII Check": [r for r in audit_rows if r['Category'] == "PII"],
        "Transactions": [r for r in audit_rows if r['Category'] == "Transactions"],
        "Transaction Mapping": tx_detail_rows,
        "Transaction Where Item Data Missing": item_error_rows,
        "Duplicate Transactions": duplicate_tx_rows
    }
