from google.oauth2 import service_account
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1beta.types import AcknowledgeUserDataCollectionRequest
from google.analytics.admin_v1beta.types import GetDataRetentionSettingsRequest, DataRetentionSettings
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, Dimension, Metric
import pandas as pd
import re
import os
import json
from dotenv import load_dotenv
from collections import Counter
import requests
import google.auth.transport.requests
import datetime

load_dotenv()

# SCOPES for both read-only and edit permissions
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly', 'https://www.googleapis.com/auth/analytics.edit']

SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
if not SERVICE_ACCOUNT_JSON:
    raise Exception("SERVICE_ACCOUNT_JSON environment variable is not set")

info = json.loads(SERVICE_ACCOUNT_JSON)
creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

# Base URL for Analytics Admin API
API_BASE_URL = "https://analyticsadmin.googleapis.com/v1beta"

def run_ga4_audit(property_numeric_id, start_date="30daysAgo", end_date="today"):
    admin_client = AnalyticsAdminServiceClient(credentials=creds)
    data_client = BetaAnalyticsDataClient(credentials=creds)

    property_id = f'properties/{property_numeric_id}'
    audit_rows = []
    item_error_rows = []
    duplicate_tx_rows = []
    purchase_log = []
    pii_found = False
    landing_page_data = []
    channel_grouping_data = []
    unassigned_source_medium_data = []

    def log(category, check, result):
        audit_rows.append({'Category': category, 'Check': check, 'Result': result})

    prop = admin_client.get_property(name=property_id)
    log("Settings", "Display Name", prop.display_name)
    log("Settings", "Time Zone", prop.time_zone)
    log("Settings", "Currency", prop.currency_code)

    # ✅ Acknowledge user data collection using direct HTTP POST
    acknowledgement_string = "I acknowledge that I have the necessary privacy disclosures and rights from my end users for the collection and processing of their data, including the association of such data with the visitation information Google Analytics collects from my site and/or app property."
    try:
        if not creds.valid:
            creds.refresh(google.auth.transport.requests.Request())

        access_token = creds.token
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        api_url = f"{API_BASE_URL}/{property_id}:acknowledgeUserDataCollection"
        request_body = {
            "acknowledgement": acknowledgement_string
        }

        response = requests.post(api_url, headers=headers, data=json.dumps(request_body))

        if response.status_code == 200:
            log("Settings", "User Data Collection Acknowledgment", "✅ Acknowledged successfully.")
        else:
            log("Settings", "User Data Collection Acknowledgment",
                f"❌ Failed to acknowledge: {response.status_code} - {response.text}")
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

    # ✅ Custom Dimension Details
    custom_dims = list(admin_client.list_custom_dimensions(parent=property_id))
    if custom_dims:
        for dim in custom_dims:
            audit_rows.append({
                'Category': 'Custom Dimension Details',
                'Check': dim.display_name,
                'Result': {
                    'Parameter Name': dim.parameter_name,
                    'Scope': dim.scope.name.replace('_', ' ').title()
                }
            })
    else:
        log("Custom Dimension Details", "No Custom Dimensions Found", "N/A")

    # ✅ Key Event Details
    key_events = list(admin_client.list_conversion_events(parent=property_id))
    if key_events:
        for event in key_events:
            formatted_create_time = datetime.datetime.fromisoformat(
                str(event.create_time).replace(' ', 'T', 1)
            ).strftime('%Y-%m-%d')

            audit_rows.append({
                'Category': 'Key Event Details',
                'Check': event.event_name,
                'Result': {
                    'Create Time': formatted_create_time,
                    'Counting Method': event.counting_method.name.replace('_', ' ').title()
                }
            })
    else:
        log("Key Event Details", "No Key Events Found", "N/A")

    # ✅ Event Inventory
    inventory_req = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}]
    )
    for row in data_client.run_report(request=inventory_req).rows:
        log("Event Inventory", row.dimension_values[0].value, row.metric_values[0].value)

    # ✅ NEW: Landing Page Analysis
    try:
        landing_page_req = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="landingPage")],
            metrics=[Metric(name="sessions")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}]
        )
        
        total_sessions = 0
        not_set_sessions = 0
        
        landing_page_response = data_client.run_report(request=landing_page_req)
        
        for row in landing_page_response.rows:
            landing_page = row.dimension_values[0].value
            sessions = int(row.metric_values[0].value)
            total_sessions += sessions
            
            landing_page_data.append({
                "Landing Page": landing_page,
                "Sessions": sessions
            })
            
            if landing_page in ["(not set)", ""]:
                not_set_sessions += sessions
        
        # Calculate percentage
        not_set_percentage = (not_set_sessions / total_sessions * 100) if total_sessions > 0 else 0
        
        log("Landing Page Analysis", "Total Sessions", total_sessions)
        log("Landing Page Analysis", "Landing Page (not set) Sessions", not_set_sessions)
        log("Landing Page Analysis", "Landing Page (not set) %", f"{not_set_percentage:.2f}%")
        
    except Exception as e:
        log("Landing Page Analysis", "Analysis Failed", str(e))

    # ✅ NEW: Session Default Channel Grouping Analysis
    try:
        channel_grouping_req = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[Metric(name="sessions")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}]
        )
        
        total_channel_sessions = 0
        unassigned_sessions = 0
        
        channel_response = data_client.run_report(request=channel_grouping_req)
        
        for row in channel_response.rows:
            channel_group = row.dimension_values[0].value
            sessions = int(row.metric_values[0].value)
            total_channel_sessions += sessions
            
            channel_grouping_data.append({
                "Channel Group": channel_group,
                "Sessions": sessions
            })
            
            if channel_group.lower() == "unassigned":
                unassigned_sessions += sessions
        
        # Calculate unassigned percentage
        unassigned_percentage = (unassigned_sessions / total_channel_sessions * 100) if total_channel_sessions > 0 else 0
        
        log("Channel Grouping Analysis", "Total Sessions", total_channel_sessions)
        log("Channel Grouping Analysis", "Unassigned Sessions", unassigned_sessions)
        log("Channel Grouping Analysis", "Unassigned %", f"{unassigned_percentage:.2f}%")
        
    except Exception as e:
        log("Channel Grouping Analysis", "Analysis Failed", str(e))

    # ✅ NEW: Unassigned Traffic - Source/Medium Breakdown
    try:
        unassigned_source_medium_req = RunReportRequest(
            property=property_id,
            dimensions=[
                Dimension(name="sessionDefaultChannelGroup"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium")
            ],
            metrics=[Metric(name="sessions")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
            dimension_filter={
                "filter": {
                    "field_name": "sessionDefaultChannelGroup",
                    "string_filter": {
                        "match_type": "EXACT",
                        "value": "Unassigned"
                    }
                }
            }
        )
        
        unassigned_response = data_client.run_report(request=unassigned_source_medium_req)
        
        for row in unassigned_response.rows:
            channel_group = row.dimension_values[0].value
            source = row.dimension_values[1].value
            medium = row.dimension_values[2].value
            sessions = int(row.metric_values[0].value)
            
            unassigned_source_medium_data.append({
                "Channel Group": channel_group,
                "Source": source,
                "Medium": medium,
                "Sessions": sessions
            })
        
        if unassigned_source_medium_data:
            log("Unassigned Traffic Details", "Source/Medium Count", len(unassigned_source_medium_data))
        else:
            log("Unassigned Traffic Details", "Source/Medium", "✅ No unassigned traffic found")
            
    except Exception as e:
        log("Unassigned Traffic Details", "Analysis Failed", str(e))

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
            purchase_log.append({"transactionId": tid, "revenue": revenue, "source": "Revenue Table"})
            if count > 1:
                duplicate_tx_rows.append({"transactionId": tid, "count": count})
    log("Transactions", "Total Unique transactionId", len(transaction_ids))
    log("Transactions", "Duplicate Transaction Count", len(duplicate_tx_rows))
    log("Transactions", "Duplicate Transaction IDs", duplicate_tx_rows if duplicate_tx_rows else "✅ No duplicates found")

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
            revenue = float(row.metric_values[0].value)

            if event_name == "purchase":
                purchase_log.append({"transactionId": tid, "itemId": item_id, "itemName": item_name, "revenue": revenue, "source": "Item Table"})
                if tid:
                    item_transaction_ids.add(tid)
                    if item_name in ["", "(not set)"] and revenue > 0:
                        item_error_rows.append({"transactionId": tid, "itemId": item_id, "itemName": item_name, "revenue": revenue})

    except Exception as e:
        log("Transactions", "Item-level check failed", str(e))

    # ✅ Compare mapping
    revenue_only_tids = list(transaction_ids - item_transaction_ids)
    items_only_tids = list(item_transaction_ids - transaction_ids)

    if revenue_only_tids:
        log("Transactions", "With Revenue but Missing Items", revenue_only_tids)
    else:
        log("Transactions", "With Revenue but Missing Items", "✅ All revenue transactions are linked to items.")

    if items_only_tids:
        log("Transactions", "With Items but No Revenue", items_only_tids)
    else:
        log("Transactions", "With Items but No Revenue", "✅ All item transactions have matching revenue data.")

    return {
        "Property Details": [r for r in audit_rows if r['Category'] == "Settings"],
        "Streams Configuration": [r for r in audit_rows if r['Category'] == "Streams"],
        "GA4 Property Limits": [r for r in audit_rows if r['Category'] == "Limits"],
        "Custom Dimension Details": [r for r in audit_rows if r['Category'] == "Custom Dimension Details"],
        "Key Event Details": [r for r in audit_rows if r['Category'] == "Key Event Details"],
        "GA4 Events": [r for r in audit_rows if r['Category'] == "Event Inventory"],
        "Landing Page Analysis": [r for r in audit_rows if r['Category'] == "Landing Page Analysis"],
        "Landing Page Data": landing_page_data,
        "Channel Grouping Analysis": [r for r in audit_rows if r['Category'] == "Channel Grouping Analysis"],
        "Channel Grouping Data": channel_grouping_data,
        "Unassigned Traffic Details": [r for r in audit_rows if r['Category'] == "Unassigned Traffic Details"],
        "Unassigned Source/Medium Data": unassigned_source_medium_data,
        "PII Check": [r for r in audit_rows if r['Category'] == "PII"],
        "Transactions": [r for r in audit_rows if r['Category'] == "Transactions"],
        "Transaction Mapping": purchase_log,
        "Transaction Where Item Data Missing": item_error_rows,
        "Duplicate Transactions": duplicate_tx_rows,
        "Revenue Only Transactions": revenue_only_tids,
        "Items Only Transactions": items_only_tids
}