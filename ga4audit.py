from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, Dimension, Metric
import re
import json
import datetime
import requests
import google.auth.transport.requests
from collections import Counter

API_BASE_URL = "https://analyticsadmin.googleapis.com/v1beta"


def run_ga4_audit_with_creds(creds, property_numeric_id, start_date="30daysAgo", end_date="today"):
    """
    Runs the full GA4 audit using the provided OAuth2 Credentials object.
    This replaces the old service-account-based run_ga4_audit().
    """
    admin_client = AnalyticsAdminServiceClient(credentials=creds)
    data_client  = BetaAnalyticsDataClient(credentials=creds)

    property_id = f"properties/{property_numeric_id}"

    audit_rows                   = []
    item_error_rows              = []
    duplicate_tx_rows            = []
    purchase_log                 = []
    pii_found                    = False
    landing_page_data            = []
    channel_grouping_data        = []
    unassigned_source_medium_data = []

    def log(category, check, result):
        audit_rows.append({"Category": category, "Check": check, "Result": result})

    # ── Property Settings ──────────────────────────────────────────────────
    prop = admin_client.get_property(name=property_id)
    log("Settings", "Display Name", prop.display_name)
    log("Settings", "Time Zone", prop.time_zone)
    log("Settings", "Currency", prop.currency_code)

    # User Data Collection Acknowledgment
    acknowledgement_string = (
        "I acknowledge that I have the necessary privacy disclosures and rights from my end users "
        "for the collection and processing of their data, including the association of such data "
        "with the visitation information Google Analytics collects from my site and/or app property."
    )
    try:
        if not creds.valid:
            creds.refresh(google.auth.transport.requests.Request())
        headers = {
            "Authorization": f"Bearer {creds.token}",
            "Content-Type": "application/json",
        }
        resp = requests.post(
            f"{API_BASE_URL}/{property_id}:acknowledgeUserDataCollection",
            headers=headers,
            data=json.dumps({"acknowledgement": acknowledgement_string}),
        )
        if resp.status_code == 200:
            log("Settings", "User Data Collection Acknowledgment", "✅ Acknowledged successfully.")
        else:
            log("Settings", "User Data Collection Acknowledgment",
                f"❌ Failed: {resp.status_code} - {resp.text}")
    except Exception as e:
        log("Settings", "User Data Collection Acknowledgment", f"❌ Failed: {e}")

    # Retention
    try:
        retention = admin_client.get_data_retention_settings(
            name=f"properties/{property_numeric_id}/dataRetentionSettings"
        )
        log("Settings", "Retention Period",
            retention.event_data_retention.name.replace("_", " ").title())
    except Exception as e:
        log("Settings", "Retention Period", f"Not available ({e})")

    # ── Streams ────────────────────────────────────────────────────────────
    for stream in admin_client.list_data_streams(parent=property_id):
        stream_type = (
            "Web" if stream.web_stream_data else
            "Android" if stream.android_app_stream_data else
            "iOS" if stream.ios_app_stream_data else "Unknown"
        )
        log("Streams", f"{stream.display_name or 'Unnamed'} ({stream_type})", stream.name)

    # ── Limits ─────────────────────────────────────────────────────────────
    log("Limits", "Custom Dimensions Used",
        f"{len(list(admin_client.list_custom_dimensions(parent=property_id)))} / 50")
    log("Limits", "Custom Metrics Used",
        f"{len(list(admin_client.list_custom_metrics(parent=property_id)))} / 50")
    log("Limits", "Key Events Used",
        f"{len(list(admin_client.list_conversion_events(parent=property_id)))} / 50")
    log("Limits", "Audiences Used",
        f"{len(list(admin_client.list_audiences(parent=property_id)))} / 100")

    # ── Custom Dimension Details ───────────────────────────────────────────
    custom_dims = list(admin_client.list_custom_dimensions(parent=property_id))
    if custom_dims:
        for dim in custom_dims:
            audit_rows.append({
                "Category": "Custom Dimension Details",
                "Check": dim.display_name,
                "Result": {
                    "Parameter Name": dim.parameter_name,
                    "Scope": dim.scope.name.replace("_", " ").title(),
                },
            })
    else:
        log("Custom Dimension Details", "No Custom Dimensions Found", "N/A")

    # ── Key Event Details ──────────────────────────────────────────────────
    key_events = list(admin_client.list_conversion_events(parent=property_id))
    if key_events:
        for event in key_events:
            formatted_time = datetime.datetime.fromisoformat(
                str(event.create_time).replace(" ", "T", 1)
            ).strftime("%Y-%m-%d")
            audit_rows.append({
                "Category": "Key Event Details",
                "Check": event.event_name,
                "Result": {
                    "Create Time": formatted_time,
                    "Counting Method": event.counting_method.name.replace("_", " ").title(),
                },
            })
    else:
        log("Key Event Details", "No Key Events Found", "N/A")

    # ── Event Inventory ────────────────────────────────────────────────────
    inv_req = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
    )
    for row in data_client.run_report(request=inv_req).rows:
        log("Event Inventory", row.dimension_values[0].value, row.metric_values[0].value)

    # ── Landing Page Analysis ──────────────────────────────────────────────
    try:
        lp_req = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="landingPage")],
            metrics=[Metric(name="sessions")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
        )
        total_sessions = 0
        not_set_sessions = 0
        for row in data_client.run_report(request=lp_req).rows:
            lp = row.dimension_values[0].value
            s  = int(row.metric_values[0].value)
            total_sessions += s
            landing_page_data.append({"Landing Page": lp, "Sessions": s})
            if lp in ["(not set)", ""]:
                not_set_sessions += s
        not_set_pct = (not_set_sessions / total_sessions * 100) if total_sessions > 0 else 0
        log("Landing Page Analysis", "Total Sessions", total_sessions)
        log("Landing Page Analysis", "Landing Page (not set) Sessions", not_set_sessions)
        log("Landing Page Analysis", "Landing Page (not set) %", f"{not_set_pct:.2f}%")
    except Exception as e:
        log("Landing Page Analysis", "Analysis Failed", str(e))

    # ── Channel Grouping Analysis ──────────────────────────────────────────
    try:
        ch_req = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[Metric(name="sessions")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
        )
        total_ch = 0
        unassigned = 0
        for row in data_client.run_report(request=ch_req).rows:
            cg = row.dimension_values[0].value
            s  = int(row.metric_values[0].value)
            total_ch += s
            channel_grouping_data.append({"Channel Group": cg, "Sessions": s})
            if cg.lower() == "unassigned":
                unassigned += s
        ua_pct = (unassigned / total_ch * 100) if total_ch > 0 else 0
        log("Channel Grouping Analysis", "Total Sessions", total_ch)
        log("Channel Grouping Analysis", "Unassigned Sessions", unassigned)
        log("Channel Grouping Analysis", "Unassigned %", f"{ua_pct:.2f}%")
    except Exception as e:
        log("Channel Grouping Analysis", "Analysis Failed", str(e))

    # ── Unassigned Source/Medium Breakdown ─────────────────────────────────
    try:
        ua_req = RunReportRequest(
            property=property_id,
            dimensions=[
                Dimension(name="sessionDefaultChannelGroup"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
            ],
            metrics=[Metric(name="sessions")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
            dimension_filter={
                "filter": {
                    "field_name": "sessionDefaultChannelGroup",
                    "string_filter": {"match_type": "EXACT", "value": "Unassigned"},
                }
            },
        )
        for row in data_client.run_report(request=ua_req).rows:
            unassigned_source_medium_data.append({
                "Channel Group": row.dimension_values[0].value,
                "Source":        row.dimension_values[1].value,
                "Medium":        row.dimension_values[2].value,
                "Sessions":      int(row.metric_values[0].value),
            })
        if unassigned_source_medium_data:
            log("Unassigned Traffic Details", "Source/Medium Count", len(unassigned_source_medium_data))
        else:
            log("Unassigned Traffic Details", "Source/Medium", "✅ No unassigned traffic found")
    except Exception as e:
        log("Unassigned Traffic Details", "Analysis Failed", str(e))

    # ── PII Check ──────────────────────────────────────────────────────────
    for dim in ["pagePath", "pageLocation"]:
        try:
            pii_req = RunReportRequest(
                property=property_id,
                dimensions=[Dimension(name=dim)],
                metrics=[Metric(name="eventCount")],
                date_ranges=[{"start_date": start_date, "end_date": end_date}],
            )
            for row in data_client.run_report(request=pii_req).rows:
                val = row.dimension_values[0].value
                if re.search(r"gmail\.com|email=|phone=|pno=|\+91\d{10}|\d{10}", val):
                    log("PII", f"Potential PII in {dim}", val)
                    pii_found = True
        except Exception:
            continue
    if not pii_found:
        log("PII", "Scan Result", "✅ No potential PII found in page paths or URLs.")

    # ── Transactions ───────────────────────────────────────────────────────
    transaction_ids    = set()
    transaction_counts = Counter()

    tx_req = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="transactionId")],
        metrics=[Metric(name="transactions"), Metric(name="purchaseRevenue")],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
    )
    for row in data_client.run_report(request=tx_req).rows:
        tid     = row.dimension_values[0].value
        count   = int(row.metric_values[0].value)
        revenue = row.metric_values[1].value
        if tid:
            transaction_ids.add(tid)
            transaction_counts[tid] += count
            purchase_log.append({"transactionId": tid, "revenue": revenue, "source": "Revenue Table"})
            if count > 1:
                duplicate_tx_rows.append({"transactionId": tid, "count": count})

    log("Transactions", "Total Unique transactionId", len(transaction_ids))
    log("Transactions", "Duplicate Transaction Count", len(duplicate_tx_rows))
    log("Transactions", "Duplicate Transaction IDs",
        duplicate_tx_rows if duplicate_tx_rows else "✅ No duplicates found")

    # ── Item-level check ───────────────────────────────────────────────────
    item_transaction_ids = set()
    try:
        item_req = RunReportRequest(
            property=property_id,
            dimensions=[
                Dimension(name="eventName"),
                Dimension(name="transactionId"),
                Dimension(name="itemId"),
                Dimension(name="itemName"),
            ],
            metrics=[Metric(name="itemRevenue")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
        )
        for row in data_client.run_report(request=item_req).rows:
            event_name = row.dimension_values[0].value
            tid        = row.dimension_values[1].value
            item_id    = row.dimension_values[2].value
            item_name  = row.dimension_values[3].value
            revenue    = float(row.metric_values[0].value)
            if event_name == "purchase":
                purchase_log.append({
                    "transactionId": tid, "itemId": item_id,
                    "itemName": item_name, "revenue": revenue, "source": "Item Table",
                })
                if tid:
                    item_transaction_ids.add(tid)
                    if item_name in ["", "(not set)"] and revenue > 0:
                        item_error_rows.append({
                            "transactionId": tid, "itemId": item_id,
                            "itemName": item_name, "revenue": revenue,
                        })
    except Exception as e:
        log("Transactions", "Item-level check failed", str(e))

    revenue_only_tids = list(transaction_ids - item_transaction_ids)
    items_only_tids   = list(item_transaction_ids - transaction_ids)

    log("Transactions", "With Revenue but Missing Items",
        revenue_only_tids if revenue_only_tids else "✅ All revenue transactions are linked to items.")
    log("Transactions", "With Items but No Revenue",
        items_only_tids if items_only_tids else "✅ All item transactions have matching revenue data.")

    # ── Return ─────────────────────────────────────────────────────────────
    return {
        "Property Details":               [r for r in audit_rows if r["Category"] == "Settings"],
        "Streams Configuration":          [r for r in audit_rows if r["Category"] == "Streams"],
        "GA4 Property Limits":            [r for r in audit_rows if r["Category"] == "Limits"],
        "Custom Dimension Details":       [r for r in audit_rows if r["Category"] == "Custom Dimension Details"],
        "Key Event Details":              [r for r in audit_rows if r["Category"] == "Key Event Details"],
        "GA4 Events":                     [r for r in audit_rows if r["Category"] == "Event Inventory"],
        "Landing Page Analysis":          [r for r in audit_rows if r["Category"] == "Landing Page Analysis"],
        "Landing Page Data":              landing_page_data,
        "Channel Grouping Analysis":      [r for r in audit_rows if r["Category"] == "Channel Grouping Analysis"],
        "Channel Grouping Data":          channel_grouping_data,
        "Unassigned Traffic Details":     [r for r in audit_rows if r["Category"] == "Unassigned Traffic Details"],
        "Unassigned Source/Medium Data":  unassigned_source_medium_data,
        "PII Check":                      [r for r in audit_rows if r["Category"] == "PII"],
        "Transactions":                   [r for r in audit_rows if r["Category"] == "Transactions"],
        "Transaction Mapping":            purchase_log,
        "Transaction Where Item Data Missing": item_error_rows,
        "Duplicate Transactions":         duplicate_tx_rows,
        "Revenue Only Transactions":      revenue_only_tids,
        "Items Only Transactions":        items_only_tids,
    }