from google_auth_oauthlib.flow import InstalledAppFlow
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, Dimension, Metric
import pandas as pd
import re
from datetime import datetime

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
creds = flow.run_local_server(port=0)
admin_client = AnalyticsAdminServiceClient(credentials=creds)
data_client = BetaAnalyticsDataClient(credentials=creds)

property_numeric_id = input("Enter GA4 Property ID (numeric only): ")
start_date = input("Enter start date (YYYY-MM-DD or '30daysAgo') [default: 30daysAgo]: ") or "30daysAgo"
end_date = input("Enter end date (YYYY-MM-DD or 'today') [default: today]: ") or "today"
property_id = f'properties/{property_numeric_id}'

audit_rows = []
tx_detail_rows = []
item_error_rows = []

pii_found = False

def log(category, check, result):
    print(f"{category} | {check}: {result}")
    audit_rows.append({'Category': category, 'Check': check, 'Result': result})

prop = admin_client.get_property(name=property_id)
log("Settings", "Display Name", prop.display_name)
log("Settings", "Time Zone", prop.time_zone)
log("Settings", "Currency", prop.currency_code)
log("Settings", "Reporting Identity", "Not available via API")
log("Settings", "Retention Period (Days)", "Not available via API")

streams = admin_client.list_data_streams(parent=property_id)
for stream in streams:
    stream_type = "Web" if stream.web_stream_data else "Android" if stream.android_app_stream_data else "iOS" if stream.ios_app_stream_data else "Unknown"
    stream_name = stream.display_name or "Unnamed Stream"
    log("Streams", f"{stream_name} ({stream_type})", stream.name)

custom_dims = list(admin_client.list_custom_dimensions(parent=property_id))
custom_metrics = list(admin_client.list_custom_metrics(parent=property_id))
key_events = list(admin_client.list_conversion_events(parent=property_id))
audiences = list(admin_client.list_audiences(parent=property_id))
log("Limits", "Custom Dimensions Used", f"{len(custom_dims)} / 50")
log("Limits", "Custom Metrics Used", f"{len(custom_metrics)} / 50")
log("Limits", "Key Events Used", f"{len(key_events)} / 50")
log("Limits", "Audiences Used", f"{len(audiences)} / 100")

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
            if re.search(r"gmail\.com|email=|phone=|pno=|\+91\d{10}|\d{10}", val):
                log("PII", f"Potential PII in {dim}", val)
                pii_found = True
    except Exception as e:
        log("PII", f"{dim} scan failed", str(e))

if not pii_found:
    log("PII", "Scan Result", "✅ No potential PII found in page paths or URLs.")

transaction_ids = set()
transaction_report = RunReportRequest(
    property=property_id,
    dimensions=[Dimension(name="transactionId")],
    metrics=[Metric(name="purchaseRevenue")],
    date_ranges=[{"start_date": start_date, "end_date": end_date}]
)
response = data_client.run_report(request=transaction_report)
for row in response.rows:
    tid = row.dimension_values[0].value
    revenue = row.metric_values[0].value
    if tid:
        transaction_ids.add(tid)
        tx_detail_rows.append({"transactionId": tid, "revenue": revenue, "source": "Revenue Table"})
    else:
        log("Transactions", "Missing transactionId", "Detected")
log("Transactions", "Total Unique transactionId", len(transaction_ids))

item_transaction_ids = set()
try:
    item_report = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="eventName"), Dimension(name="transactionId"), Dimension(name="itemId"), Dimension(name="itemName")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        limit=1000
    )
    response = data_client.run_report(request=item_report)
    for row in response.rows:
        event_name = row.dimension_values[0].value
        tid = row.dimension_values[1].value
        item_id = row.dimension_values[2].value
        item_name = row.dimension_values[3].value
        if event_name == "purchase":
            if tid and tid != "(not set)":
                item_transaction_ids.add(tid)
                tx_detail_rows.append({"transactionId": tid, "itemId": item_id, "itemName": item_name, "source": "Item Table"})
            else:
                log("Transactions", "Item missing valid transactionId", f"Item ID: {item_id} / Name: {item_name}")
                item_error_rows.append({"itemId": item_id, "itemName": item_name, "transactionId": tid})
    missing_in_items = transaction_ids - item_transaction_ids
    missing_in_txns = item_transaction_ids - transaction_ids
    if not missing_in_items:
        log("Transactions", "With Revenue but Missing Items", "✅ All revenue transactions are linked to items.")
    else:
        log("Transactions", "With Revenue but Missing Items", str(missing_in_items))
    if not missing_in_txns:
        log("Transactions", "With Items but No Revenue", "✅ All item transactions have matching revenue data.")
    else:
        log("Transactions", "With Items but No Revenue", str(missing_in_txns))
except Exception as e:
    log("Transactions", "Item-level check failed", str(e))

df = pd.DataFrame(audit_rows)
tx_df = pd.DataFrame(tx_detail_rows)
error_df = pd.DataFrame(item_error_rows)
with pd.ExcelWriter("GA4_Audit_Report.xlsx") as writer:
    df.to_excel(writer, sheet_name="Audit Summary", index=False)
    if not tx_df.empty:
        tx_df.to_excel(writer, sheet_name="Transaction Mapping", index=False)
    if not error_df.empty:
        error_df.to_excel(writer, sheet_name="Item Errors", index=False)

print("\n✅ Audit completed. Results saved to GA4_Audit_Report.xlsx")
