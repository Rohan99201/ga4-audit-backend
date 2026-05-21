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

# ── PII Detection ──────────────────────────────────────────────────────────
# Only flag parameters that carry actual personal data.
# UTMs, click IDs (gclid, fbclid, gbraid, gad_*), campaign IDs are NOT PII.

PII_PARAM_NAMES = re.compile(
    r"(?:^|[?&])"
    r"(?:email|e-mail|mail|fname|first_name|firstname|lname|last_name|lastname"
    r"|fullname|full_name|name|phone|mobile|tel|pno|user_id|userid|uid|customerid"
    r"|customer_id|member_id|memberid|subscriber_id|account_id)"
    r"=([^&]+)",
    re.IGNORECASE,
)

PII_EMAIL_IN_PATH = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

REDACTED_MARKER = "(redacted)"  # GA4 built-in PII redaction

# Parameters that are never PII regardless of name
NON_PII_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "gbraid", "wbraid", "msclkid", "ttclid", "li_fat_id",
    "gad_source", "gad_campaignid", "gclsrc", "campid", "adid", "groupid",
    "uadgroup", "uadcampaign", "lsid", "lssid", "tgid", "network", "device",
    "type", "ad_pos", "fdid", "ptid", "lphys", "linst", "model", "sitetarg",
}


def is_pii(url_or_path: str) -> tuple[bool, str]:
    """
    Returns (True, reason) if actual PII is detected, else (False, "").
    Skips GA4-redacted values, UTMs, and click IDs.
    """
    if not url_or_path or REDACTED_MARKER in url_or_path:
        return False, ""

    # Check for raw email addresses embedded directly in the path/URL
    email_match = PII_EMAIL_IN_PATH.search(url_or_path)
    if email_match:
        return True, f"Email address found: {email_match.group()}"

    # Check for PII parameter names with non-empty, non-redacted values
    for match in PII_PARAM_NAMES.finditer(url_or_path):
        param_full = match.group(0).lstrip("?&")
        param_name = param_full.split("=")[0].lower()
        param_value = match.group(1)

        # Skip if it's a known non-PII param
        if param_name in NON_PII_PARAMS:
            continue
        # Skip if value is empty, (not set), or redacted
        if not param_value or param_value in ["(not set)", "(redacted)", REDACTED_MARKER]:
            continue
        # Skip if value looks like a numeric ID only (campaign/ad IDs)
        if re.match(r"^\d+$", param_value) and param_name in {"id", "uid", "userid"}:
            continue

        return True, f"PII parameter detected: {param_name}={param_value[:40]}"

    return False, ""


def run_ga4_audit_with_creds(creds, property_numeric_id, start_date="30daysAgo", end_date="today"):
    admin_client = AnalyticsAdminServiceClient(credentials=creds)
    data_client  = BetaAnalyticsDataClient(credentials=creds)

    property_id = f"properties/{property_numeric_id}"

    audit_rows                    = []
    item_error_rows               = []
    duplicate_tx_rows             = []
    purchase_log                  = []
    pii_found                     = False
    landing_page_data             = []
    channel_grouping_data         = []
    unassigned_source_medium_data = []

    def log(category, check, result):
        audit_rows.append({"Category": category, "Check": check, "Result": result})

    # ── Property Settings ──────────────────────────────────────────────────
    prop = admin_client.get_property(name=property_id)
    log("Settings", "Display Name",  prop.display_name)
    log("Settings", "Property ID",   property_numeric_id)

    # Industry Category — SDK returns an IndustryCategory enum with a .name attribute
    try:
        raw_ic = prop.industry_category          # e.g. IndustryCategory.SHOPPING
        ic_name = raw_ic.name                    # e.g. "SHOPPING"
        if ic_name in ("", "INDUSTRY_CATEGORY_UNSPECIFIED"):
            ic_label = "[Not set]"
        else:
            ic_label = ic_name.replace("_", " ").title()  # e.g. "Shopping"
    except Exception:
        ic_label = "[Not set]"
    log("Settings", "Industry Category", ic_label)

    log("Settings", "Time Zone", prop.time_zone)
    log("Settings", "Currency",  prop.currency_code)

    acknowledgement_string = (
        "I acknowledge that I have the necessary privacy disclosures and rights from my end users "
        "for the collection and processing of their data, including the association of such data "
        "with the visitation information Google Analytics collects from my site and/or app property."
    )
    try:
        if not creds.valid:
            creds.refresh(google.auth.transport.requests.Request())
        headers = {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}
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

    try:
        retention = admin_client.get_data_retention_settings(
            name=f"properties/{property_numeric_id}/dataRetentionSettings"
        )
        log("Settings", "Retention Period",
            retention.event_data_retention.name.replace("_", " ").title())
    except Exception as e:
        log("Settings", "Retention Period", f"Not available ({e})")

    # ── Streams — full details from list_data_streams API ─────────────────
    try:
        streams_list = list(admin_client.list_data_streams(parent=property_id))
    except Exception as e:
        streams_list = []
        log("Streams", "Stream Error", f"Could not fetch streams: {e}")

    for stream in streams_list:
        # Resource name: "properties/123/dataStreams/456" → numeric ID is last segment
        try:
            stream_numeric_id = stream.name.split("/")[-1]
        except Exception:
            stream_numeric_id = "[unknown]"

        # Stream display name
        stream_display = stream.display_name or "[Unnamed]"

        # Type-specific fields
        if stream.web_stream_data and stream.web_stream_data.measurement_id:
            stream_type    = "Web"
            stream_url     = stream.web_stream_data.default_uri or "[not set]"
            measurement_id = stream.web_stream_data.measurement_id  # e.g. "G-XXXXXXXX"
        elif stream.android_app_stream_data:
            stream_type    = "Android App"
            stream_url     = getattr(stream.android_app_stream_data, "package_name", "[not set]") or "[not set]"
            measurement_id = "[N/A — App stream]"
        elif stream.ios_app_stream_data:
            stream_type    = "iOS App"
            stream_url     = getattr(stream.ios_app_stream_data, "bundle_id", "[not set]") or "[not set]"
            measurement_id = "[N/A — App stream]"
        else:
            # Fallback: try to read web fields directly even if check failed
            try:
                measurement_id = stream.web_stream_data.measurement_id or "[not set]"
                stream_url     = stream.web_stream_data.default_uri or "[not set]"
                stream_type    = "Web"
            except Exception:
                stream_type    = "Unknown"
                stream_url     = "[not set]"
                measurement_id = "[not set]"

        log("Streams", "Stream Name",    stream_display)
        log("Streams", "Stream Type",    stream_type)
        log("Streams", "Stream ID",      stream_numeric_id)
        log("Streams", "Stream URL",     stream_url)
        log("Streams", "Measurement ID", measurement_id)

    # ── Google Ads Links ───────────────────────────────────────────────────
    try:
        google_ads_links = list(admin_client.list_google_ads_links(parent=property_id))
        if google_ads_links:
            for link in google_ads_links:
                ads_personalisation = "✅ Enabled" if link.ads_personalization_enabled else "❌ Disabled"
                log("Product Links", "Google Ads",
                    f"✅ Linked — Customer ID: {link.customer_id} | Ads Personalisation: {ads_personalisation}")
        else:
            log("Product Links", "Google Ads", "❌ Not linked")
    except Exception as e:
        log("Product Links", "Google Ads", f"⚠️ Could not check: {e}")

    # ── Firebase Links ─────────────────────────────────────────────────────
    try:
        firebase_links = list(admin_client.list_firebase_links(parent=property_id))
        if firebase_links:
            for link in firebase_links:
                log("Product Links", "Firebase",
                    f"✅ Linked — Project: {link.project}")
        else:
            log("Product Links", "Firebase", "❌ Not linked")
    except Exception as e:
        log("Product Links", "Firebase", f"⚠️ Could not check: {e}")

    # ── Limits (dynamic based on service level) ────────────────────────────
    # Detect 360 vs Standard from the property object
    is_360 = (
        hasattr(prop, "service_level") and
        str(prop.service_level) in ("ServiceLevel.GOOGLE_ANALYTICS_360", "2", "GOOGLE_ANALYTICS_360")
    )
    tier_label = "GA4 360" if is_360 else "Standard"
    log("Settings", "Service Level", f"{'✅ Google Analytics 360' if is_360 else '📊 Google Analytics Standard'}")

    # Limits table per tier (from GA4 docs)
    # Standard: user-scoped dims=25, event-scoped dims=50, item-scoped dims=10, metrics=50, calc metrics=5
    # 360:      user-scoped dims=100, event-scoped dims=125, item-scoped dims=25, metrics=125, calc metrics=50
    custom_dims_list   = list(admin_client.list_custom_dimensions(parent=property_id))
    custom_metrics_list = list(admin_client.list_custom_metrics(parent=property_id))
    key_events_list    = list(admin_client.list_conversion_events(parent=property_id))
    audiences_list     = list(admin_client.list_audiences(parent=property_id))

    # Split custom dims by scope — scope is an integer enum: 1=EVENT, 2=USER, 3=ITEM
    user_dims  = [d for d in custom_dims_list if int(d.scope) == 2]
    event_dims = [d for d in custom_dims_list if int(d.scope) == 1]
    item_dims  = [d for d in custom_dims_list if int(d.scope) == 3]

    user_dim_limit  = 100 if is_360 else 25
    event_dim_limit = 125 if is_360 else 50
    item_dim_limit  = 25  if is_360 else 10
    metric_limit    = 125 if is_360 else 50
    key_event_limit = 50   # same for both tiers
    audience_limit  = 100  # same for both tiers

    log("Limits", "User-Scoped Custom Dimensions Used",
        f"{len(user_dims)} / {user_dim_limit} ({tier_label})")
    log("Limits", "Event-Scoped Custom Dimensions Used",
        f"{len(event_dims)} / {event_dim_limit} ({tier_label})")
    log("Limits", "Item-Scoped Custom Dimensions Used",
        f"{len(item_dims)} / {item_dim_limit} ({tier_label})")
    log("Limits", "Custom Metrics Used",
        f"{len(custom_metrics_list)} / {metric_limit} ({tier_label})")
    log("Limits", "Key Events Used",
        f"{len(key_events_list)} / {key_event_limit}")
    log("Limits", "Audiences Used",
        f"{len(audiences_list)} / {audience_limit}")

    # ── Custom Dimension Details — split by scope ──────────────────────────
    if custom_dims_list:
        for dim in custom_dims_list:
            scope_int = int(dim.scope)  # 1=EVENT, 2=USER, 3=ITEM
            scope_label = {1: "Event", 2: "User", 3: "Item"}.get(scope_int, str(dim.scope))
            category = {
                1: "Custom Dimensions - Event Scoped",
                2: "Custom Dimensions - User Scoped",
                3: "Custom Dimensions - Item Scoped",
            }.get(scope_int, "Custom Dimensions - Event Scoped")

            audit_rows.append({
                "Category": category,
                "Check": dim.display_name,
                "Result": {
                    "Parameter Name": dim.parameter_name,
                    "Scope": scope_label,
                    "Ads Personalization Excluded": str(getattr(dim, "disallow_ads_personalization", False)),
                },
            })
    else:
        log("Custom Dimensions - Event Scoped", "No Custom Dimensions Found", "N/A")

    # ── Key Event Details ──────────────────────────────────────────────────
    if key_events_list:
        for event in key_events_list:
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

    # ── PII Check (improved) ───────────────────────────────────────────────
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
                found, reason = is_pii(val)
                if found:
                    log("PII", f"Potential PII in {dim}", f"❌ {reason} — {val[:120]}")
                    pii_found = True
        except Exception:
            continue

    if not pii_found:
        log("PII", "Scan Result", "✅ No PII found. UTMs, click IDs and redacted values excluded.")

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

    return {
        "Property Details":                    [r for r in audit_rows if r["Category"] == "Settings"],
        "Streams Configuration":               [r for r in audit_rows if r["Category"] == "Streams"],
        "GA4 Property Limits":                 [r for r in audit_rows if r["Category"] == "Limits"],
        "Product Links":                       [r for r in audit_rows if r["Category"] == "Product Links"],
        "Custom Dimensions - Event Scoped":    [r for r in audit_rows if r["Category"] == "Custom Dimensions - Event Scoped"],
        "Custom Dimensions - User Scoped":     [r for r in audit_rows if r["Category"] == "Custom Dimensions - User Scoped"],
        "Custom Dimensions - Item Scoped":     [r for r in audit_rows if r["Category"] == "Custom Dimensions - Item Scoped"],
        "Key Event Details":                   [r for r in audit_rows if r["Category"] == "Key Event Details"],
        "GA4 Events":                          [r for r in audit_rows if r["Category"] == "Event Inventory"],
        "Landing Page Analysis":               [r for r in audit_rows if r["Category"] == "Landing Page Analysis"],
        "Landing Page Data":                   landing_page_data,
        "Channel Grouping Analysis":           [r for r in audit_rows if r["Category"] == "Channel Grouping Analysis"],
        "Channel Grouping Data":               channel_grouping_data,
        "Unassigned Traffic Details":          [r for r in audit_rows if r["Category"] == "Unassigned Traffic Details"],
        "Unassigned Source/Medium Data":       unassigned_source_medium_data,
        "PII Check":                           [r for r in audit_rows if r["Category"] == "PII"],
        "Transactions":                        [r for r in audit_rows if r["Category"] == "Transactions"],
        "Transaction Mapping":                 purchase_log,
        "Transaction Where Item Data Missing": item_error_rows,
        "Duplicate Transactions":              duplicate_tx_rows,
        "Revenue Only Transactions":           revenue_only_tids,
        "Items Only Transactions":             items_only_tids,
    }