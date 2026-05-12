from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
from google.oauth2.credentials import Credentials
from google.analytics.admin import AnalyticsAdminServiceClient
import requests
import os
import json
import base64
from ga4audit import run_ga4_audit_with_creds

app = FastAPI()

# ── Environment Variables ──────────────────────────────────────────────────
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
REDIRECT_URI         = os.getenv("REDIRECT_URI", "https://ga4-audit-backend.onrender.com/auth/callback")
FRONTEND_URL         = os.getenv("FRONTEND_URL", "https://ga4-audit-frontend.vercel.app")

GOOGLE_AUTH_URL  = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/analytics.edit",
    "openid",
    "email",
    "profile",
]

# ── CORS ───────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_URL,
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:5175",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Helper: decode token from request header ───────────────────────────────
def get_user_credentials(request: Request) -> Credentials:
    """
    Frontend sends: Authorization: Bearer <base64-encoded-token-json>
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated. Please log in.")
    try:
        token_json = base64.b64decode(auth_header[7:]).decode("utf-8")
        token_data = json.loads(token_json)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token format.")

    return Credentials(
        token=token_data["access_token"],
        refresh_token=token_data.get("refresh_token"),
        token_uri=GOOGLE_TOKEN_URL,
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=SCOPES,
    )

# ── Routes ─────────────────────────────────────────────────────────────────

@app.get("/")
def read_root():
    return {"message": "GA4 Audit API is running 🚀"}


@app.get("/auth/google")
def auth_google():
    """Step 1 — redirect user to Google consent screen."""
    scope_str = " ".join(SCOPES)
    params = (
        f"?client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_type=code"
        f"&scope={requests.utils.quote(scope_str)}"
        f"&access_type=offline"
        f"&prompt=consent"
    )
    return RedirectResponse(url=GOOGLE_AUTH_URL + params)


@app.get("/auth/callback")
def auth_callback(code: str = Query(...)):
    """
    Step 2 — exchange code for tokens, then redirect to frontend
    with the token data base64-encoded in the URL fragment (#).
    Fragments never leave the browser so the token stays client-side only.
    """
    token_response = requests.post(GOOGLE_TOKEN_URL, data={
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    })

    if token_response.status_code != 200:
        return RedirectResponse(url=f"{FRONTEND_URL}?auth=error&reason=token_exchange_failed")

    token_data = token_response.json()

    # Fetch user info
    userinfo_resp = requests.get(
        "https://www.googleapis.com/oauth2/v3/userinfo",
        headers={"Authorization": f"Bearer {token_data['access_token']}"}
    )
    user_info = userinfo_resp.json() if userinfo_resp.status_code == 200 else {}

    # Bundle token + user info, base64 encode, pass via URL fragment
    payload = json.dumps({
        "token_data": token_data,
        "user_info": user_info,
    })
    encoded = base64.urlsafe_b64encode(payload.encode()).decode()

    # Use fragment (#) — never sent to server, stays in browser only
    return RedirectResponse(url=f"{FRONTEND_URL}#auth={encoded}")


@app.get("/list-properties")
def list_properties(request: Request):
    """Returns all GA4 properties the logged-in user has access to."""
    creds = get_user_credentials(request)
    try:
        admin_client = AnalyticsAdminServiceClient(credentials=creds)
        accounts = admin_client.list_account_summaries()
        properties = []
        for account in accounts:
            for prop in account.property_summaries:
                properties.append({
                    "property_id": prop.property.replace("properties/", ""),
                    "display_name": prop.display_name,
                    "account_name": account.display_name,
                })
        return {"success": True, "properties": properties}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/run-audit")
def run_audit(
    request: Request,
    property_id: str = Query(...),
    start_date: str = Query("30daysAgo"),
    end_date: str = Query("today"),
):
    """Runs the full GA4 audit using the logged-in user's credentials."""
    creds = get_user_credentials(request)
    try:
        results = run_ga4_audit_with_creds(
            creds=creds,
            property_numeric_id=property_id,
            start_date=start_date,
            end_date=end_date,
        )
        return {"success": True, "data": results}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/sdr-report")
def sdr_report(
    request: Request,
    property_id: str = Query(...),
    start_date: str = Query("30daysAgo"),
    end_date: str = Query("today"),
    body: dict = None,
):
    """
    Fetches live GA4 data for a list of SDR events.
    POST body: { "events": ["event_name_1", "event_name_2", ...], "params": ["param1", "param2"] }
    Returns event counts and top parameter values for each requested event.
    """
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest, Dimension, Metric, FilterExpression,
        Filter, FilterExpressionList
    )

    creds = get_user_credentials(request)

    if not body:
        return {"success": False, "error": "Request body required with 'events' list."}

    event_names = body.get("events", [])
    param_names = body.get("params", [])

    if not event_names:
        return {"success": False, "error": "No events provided."}

    try:
        data_client = BetaAnalyticsDataClient(credentials=creds)
        prop = f"properties/{property_id}"
        report_rows = []

        # ── Step 1: Event counts per event name ────────────────────────────
        event_count_req = RunReportRequest(
            property=prop,
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount"), Metric(name="totalUsers")],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
            dimension_filter=FilterExpression(
                or_group=FilterExpressionList(
                    expressions=[
                        FilterExpression(filter=Filter(
                            field_name="eventName",
                            string_filter=Filter.StringFilter(
                                match_type=Filter.StringFilter.MatchType.EXACT,
                                value=evt,
                            )
                        ))
                        for evt in event_names
                    ]
                )
            ),
            limit=1000,
        )
        event_counts = {}
        event_users  = {}
        resp = data_client.run_report(request=event_count_req)
        for row in resp.rows:
            evt = row.dimension_values[0].value
            event_counts[evt] = int(row.metric_values[0].value)
            event_users[evt]  = int(row.metric_values[1].value)

        # ── Step 2: Per-event, per-param top values ────────────────────────
        param_data = {}  # { "event_name": { "param_name": [top values] } }

        for evt in event_names:
            param_data[evt] = {}
            for param in param_names:
                if not param:
                    continue
                # Use eventName + customEvent:param or just param dimension
                try:
                    param_req = RunReportRequest(
                        property=prop,
                        dimensions=[
                            Dimension(name="eventName"),
                            Dimension(name=f"customEvent:{param}"),
                        ],
                        metrics=[Metric(name="eventCount")],
                        date_ranges=[{"start_date": start_date, "end_date": end_date}],
                        dimension_filter=FilterExpression(
                            filter=Filter(
                                field_name="eventName",
                                string_filter=Filter.StringFilter(
                                    match_type=Filter.StringFilter.MatchType.EXACT,
                                    value=evt,
                                )
                            )
                        ),
                        limit=10,
                        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}],
                    )
                    param_resp = data_client.run_report(request=param_req)
                    top_values = []
                    for row in param_resp.rows:
                        val = row.dimension_values[1].value
                        cnt = int(row.metric_values[0].value)
                        if val and val not in ["(not set)", ""]:
                            top_values.append({"value": val, "count": cnt})
                    param_data[evt][param] = top_values
                except Exception:
                    param_data[evt][param] = []

        # ── Build response ─────────────────────────────────────────────────
        for evt in event_names:
            report_rows.append({
                "eventName":   evt,
                "eventCount":  event_counts.get(evt, 0),
                "totalUsers":  event_users.get(evt, 0),
                "inGA4":       evt in event_counts,
                "paramData":   param_data.get(evt, {}),
            })

        return {"success": True, "report": report_rows}

    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/explore")
def explore(
    request: Request,
    property_id: str = Query(...),
    start_date: str = Query("30daysAgo"),
    end_date: str = Query("today"),
    body: dict = None,
):
    """
    Flexible GA4 Data API explorer.
    POST body: {
      "dimensions": ["eventName", "sessionDefaultChannelGroup", ...],
      "metrics": ["eventCount", "totalUsers", "sessions", ...],
      "limit": 100,
      "order_by_metric": "eventCount"
    }
    Returns rows with dimension values and metric values.
    """
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest, Dimension, Metric, OrderBy
    )

    creds = get_user_credentials(request)

    if not body:
        return {"success": False, "error": "Request body required."}

    dimensions   = body.get("dimensions", [])
    metrics      = body.get("metrics", [])
    limit        = min(int(body.get("limit", 100)), 5000)
    order_metric = body.get("order_by_metric", metrics[0] if metrics else None)

    if not dimensions and not metrics:
        return {"success": False, "error": "At least one dimension or metric required."}

    try:
        data_client = BetaAnalyticsDataClient(credentials=creds)

        order_bys = []
        if order_metric and order_metric in metrics:
            order_bys = [OrderBy(
                metric=OrderBy.MetricOrderBy(metric_name=order_metric),
                desc=True,
            )]

        # ── Build dimension filter from body ───────────────────────────────
        filters_list = body.get("filters", [])  # [{dimension, matchType, value}]
        dimension_filter = None

        if filters_list:
            from google.analytics.data_v1beta.types import FilterExpression, FilterExpressionList, Filter

            def build_filter(f):
                match_map = {
                    "EXACT":       Filter.StringFilter.MatchType.EXACT,
                    "BEGINS_WITH": Filter.StringFilter.MatchType.BEGINS_WITH,
                    "ENDS_WITH":   Filter.StringFilter.MatchType.ENDS_WITH,
                    "CONTAINS":    Filter.StringFilter.MatchType.CONTAINS,
                    "REGEXP":      Filter.StringFilter.MatchType.FULL_REGEXP,
                }
                return FilterExpression(filter=Filter(
                    field_name=f["dimension"],
                    string_filter=Filter.StringFilter(
                        match_type=match_map.get(f.get("matchType","CONTAINS"), Filter.StringFilter.MatchType.CONTAINS),
                        value=f["value"],
                        case_sensitive=False,
                    )
                ))

            if len(filters_list) == 1:
                dimension_filter = build_filter(filters_list[0])
            else:
                dimension_filter = FilterExpression(
                    and_group=FilterExpressionList(
                        expressions=[build_filter(f) for f in filters_list]
                    )
                )

        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
            limit=limit,
            order_bys=order_bys,
            dimension_filter=dimension_filter,
        )

        resp = data_client.run_report(request=req)

        rows = []
        for row in resp.rows:
            r = {}
            for i, d in enumerate(dimensions):
                r[d] = row.dimension_values[i].value
            for i, m in enumerate(metrics):
                r[m] = row.metric_values[i].value
            rows.append(r)

        return {
            "success": True,
            "rows": rows,
            "row_count": len(rows),
            "dimensions": dimensions,
            "metrics": metrics,
        }

    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/custom-dimensions")
def get_custom_dimensions(
    request: Request,
    property_id: str = Query(...),
):
    """Returns all custom dimensions for a property to populate the Explorer dimension list."""
    from google.analytics.admin import AnalyticsAdminServiceClient
    creds = get_user_credentials(request)
    try:
        admin_client = AnalyticsAdminServiceClient(credentials=creds)
        dims = list(admin_client.list_custom_dimensions(parent=f"properties/{property_id}"))
        result = []
        for d in dims:
            scope_int = int(d.scope)
            prefix = {1: "customEvent", 2: "customUser", 3: "customItem"}.get(scope_int, "customEvent")
            scope_label = {1: "Event", 2: "User", 3: "Item"}.get(scope_int, "Event")
            result.append({
                "id":           f"{prefix}:{d.parameter_name}",
                "label":        f"{d.display_name} ({scope_label})",
                "paramName":    d.parameter_name,
                "displayName":  d.display_name,
                "scope":        scope_label,
                "group":        f"Custom — {scope_label} Scoped",
            })
        return {"success": True, "dimensions": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export/xlsx")
def export_xlsx(
    request: Request,
    body: dict = None,
):
    """
    Generates an audit report as a multi-sheet .xlsx file.
    POST body: { "data": <audit_data_object>, "property_name": "...", "date_range": "..." }
    """
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    from fastapi.responses import StreamingResponse

    get_user_credentials(request)  # auth check

    if not body:
        return {"success": False, "error": "Request body required."}

    audit_data   = body.get("data", {})
    prop_name    = body.get("property_name", "GA4 Property")
    date_range   = body.get("date_range", "")

    # ── Brainlabs colours ──────────────────────────────────────────────────
    BL_BLACK  = "0A0A0A"
    BL_YELLOW = "FFD426"
    BL_WHITE  = "FFFFFF"
    BL_DGREY  = "1A1A1A"
    BL_MGREY  = "3D3D3D"
    BL_LGREY  = "9A9A9A"
    BL_GREEN  = "00C896"
    BL_RED    = "FF4444"

    wb = Workbook()
    wb.remove(wb.active)  # remove default sheet

    def make_header_style(ws, row, cols, bg=BL_BLACK, fg=BL_YELLOW, bold=True, size=11):
        for col in range(1, cols+1):
            c = ws.cell(row=row, column=col)
            c.font = Font(name="Arial", bold=bold, color=fg, size=size)
            c.fill = PatternFill("solid", fgColor=bg)
            c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)

    def make_row_style(ws, row, cols, even=False):
        bg = "F7F6F2" if even else BL_WHITE
        for col in range(1, cols+1):
            c = ws.cell(row=row, column=col)
            c.font = Font(name="Arial", size=10, color=BL_BLACK)
            c.fill = PatternFill("solid", fgColor=bg)
            c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

    def set_col_widths(ws, widths):
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = w

    def add_title_row(ws, title, subtitle=""):
        ws.row_dimensions[1].height = 32
        ws["A1"] = title
        ws["A1"].font = Font(name="Arial", bold=True, size=14, color=BL_YELLOW)
        ws["A1"].fill = PatternFill("solid", fgColor=BL_BLACK)
        ws["A1"].alignment = Alignment(horizontal="left", vertical="center")
        if subtitle:
            ws["B1"] = subtitle
            ws["B1"].font = Font(name="Arial", size=10, color=BL_LGREY)
            ws["B1"].fill = PatternFill("solid", fgColor=BL_BLACK)
            ws["B1"].alignment = Alignment(horizontal="left", vertical="center")

    # ── Sheet builder helper ───────────────────────────────────────────────
    def add_check_result_sheet(name, entries):
        ws = wb.create_sheet(title=name[:31])
        add_title_row(ws, name, f"{prop_name} · {date_range}")
        ws.merge_cells("A1:B1")
        ws.row_dimensions[2].height = 22
        ws["A2"] = "Check"; ws["B2"] = "Result"
        make_header_style(ws, 2, 2)
        for i, e in enumerate(entries):
            r = i + 3
            result_val = e.get("Result", "")
            if isinstance(result_val, (list, dict)):
                result_val = str(result_val)
            ws.cell(row=r, column=1).value = e.get("Check", "")
            ws.cell(row=r, column=2).value = result_val
            make_row_style(ws, r, 2, even=(i % 2 == 0))
            # Colour-code results
            c = ws.cell(row=r, column=2)
            if isinstance(result_val, str):
                if "✅" in result_val:
                    c.font = Font(name="Arial", size=10, color=BL_GREEN)
                elif "❌" in result_val:
                    c.font = Font(name="Arial", size=10, color=BL_RED)
        set_col_widths(ws, [42, 55])

    def add_table_sheet(name, headers, rows_data):
        ws = wb.create_sheet(title=name[:31])
        add_title_row(ws, name, f"{prop_name} · {date_range}")
        ws.merge_cells(f"A1:{get_column_letter(len(headers))}1")
        hr = 2
        ws.row_dimensions[hr].height = 22
        for ci, h in enumerate(headers, 1):
            ws.cell(row=hr, column=ci).value = h
        make_header_style(ws, hr, len(headers))
        for i, row in enumerate(rows_data):
            r = i + 3
            for ci, val in enumerate(row, 1):
                ws.cell(row=r, column=ci).value = val
            make_row_style(ws, r, len(headers), even=(i % 2 == 0))
        col_w = max(18, 60 // max(len(headers), 1))
        set_col_widths(ws, [col_w] * len(headers))

    # ── Build sheets ───────────────────────────────────────────────────────
    sections = [
        ("Property Details",     audit_data.get("Property Details", [])),
        ("Streams Config",       audit_data.get("Streams Configuration", [])),
        ("GA4 Property Limits",  audit_data.get("GA4 Property Limits", [])),
        ("PII Check",            audit_data.get("PII Check", [])),
        ("Transactions",         audit_data.get("Transactions", [])),
        ("Landing Page Analysis",audit_data.get("Landing Page Analysis", [])),
        ("Channel Analysis",     audit_data.get("Channel Grouping Analysis", [])),
    ]
    for name, entries in sections:
        if entries:
            add_check_result_sheet(name, entries)

    # Events
    events = audit_data.get("GA4 Events", [])
    if events:
        add_table_sheet("Event Inventory", ["Event Name", "Event Count"],
                        [(e.get("Check",""), e.get("Result","")) for e in events])

    # Custom Dimensions — all scopes
    for scope in ["Event Scoped", "User Scoped", "Item Scoped"]:
        dims = audit_data.get(f"Custom Dimensions - {scope}", [])
        if dims:
            add_table_sheet(f"Custom Dims ({scope[:4]})", ["Display Name","Parameter Name","Scope","Ads Personalisation"],
                [(e.get("Check",""), e.get("Result",{}).get("Parameter Name",""), e.get("Result",{}).get("Scope",""), e.get("Result",{}).get("Ads Personalization Excluded","")) for e in dims])

    # Key Events
    key_events = audit_data.get("Key Event Details", [])
    if key_events:
        add_table_sheet("Key Events", ["Event Name","Create Time","Counting Method"],
                [(e.get("Check",""), e.get("Result",{}).get("Create Time",""), e.get("Result",{}).get("Counting Method","")) for e in key_events])

    # Duplicate Transactions
    dup_txns = audit_data.get("Duplicate Transactions", [])
    if dup_txns:
        add_table_sheet("Duplicate Transactions", ["Transaction ID","Count"],
                        [(e.get("transactionId",""), e.get("count","")) for e in dup_txns])

    # Landing Page Data
    lp_data = audit_data.get("Landing Page Data", [])
    if lp_data:
        add_table_sheet("Landing Page Data", ["Landing Page","Sessions"],
                        [(e.get("Landing Page",""), e.get("Sessions","")) for e in lp_data[:200]])

    # Channel Data
    ch_data = audit_data.get("Channel Grouping Data", [])
    if ch_data:
        add_table_sheet("Channel Grouping", ["Channel Group","Sessions"],
                        [(e.get("Channel Group",""), e.get("Sessions","")) for e in ch_data])

    # Unassigned source/medium
    ua_data = audit_data.get("Unassigned Source/Medium Data", [])
    if ua_data:
        add_table_sheet("Unassigned Traffic", ["Channel","Source","Medium","Sessions"],
                        [(e.get("Channel Group",""),e.get("Source",""),e.get("Medium",""),e.get("Sessions","")) for e in ua_data])

    # ── Stream to response ─────────────────────────────────────────────────
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    filename = f"GA4_Audit_{prop_name.replace(' ','_')}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.post("/export/pptx")
def export_pptx(
    request: Request,
    body: dict = None,
):
    """
    Generates an audit report as a Brainlabs-branded .pptx file.
    POST body: { "data": <audit_data>, "property_name": "...", "date_range": "..." }
    """
    import io, subprocess, tempfile, os
    from fastapi.responses import StreamingResponse

    get_user_credentials(request)  # auth check

    if not body:
        return {"success": False, "error": "Request body required."}

    audit_data  = body.get("data", {})
    prop_name   = body.get("property_name", "GA4 Property")
    date_range  = body.get("date_range", "30daysAgo – today")

    # Build JS script inline
    def safe_str(v):
        if v is None: return ""
        s = str(v)
        return s.replace("\\","\\\\").replace('"','\\"').replace("\n"," ").replace("\r","")

    def make_table_rows(entries, cols):
        """Build a JSON-serialisable 2D array for pptxgenjs addTable."""
        rows = []
        for e in entries[:20]:  # cap at 20 rows per slide for legibility
            row = []
            for c in cols:
                val = e.get(c, "")
                if isinstance(val, dict):
                    val = " | ".join(f"{k}: {v}" for k, v in val.items())
                elif isinstance(val, list):
                    val = str(val)[:80]
                row.append(safe_str(val))
            rows.append(row)
        return rows

    # Summary bullets
    summary_items = []
    if audit_data.get("Duplicate Transactions"): summary_items.append({"text":"❌ Duplicate transactions found","ok":False})
    if audit_data.get("Transaction Where Item Data Missing"): summary_items.append({"text":"❌ Purchase events with missing item names","ok":False})
    if any(e.get("Result","").startswith("❌") for e in audit_data.get("PII Check",[])):
        summary_items.append({"text":"❌ Potential PII detected in page paths","ok":False})
    lp_pct = next((e.get("Result","0%") for e in audit_data.get("Landing Page Analysis",[]) if e.get("Check")=="Landing Page (not set) %"), "0%")
    if float(str(lp_pct).replace("%","") or 0) > 10:
        summary_items.append({"text":f"⚠️ Landing Page (not set) rate: {lp_pct}","ok":None})
    ua_pct = next((e.get("Result","0%") for e in audit_data.get("Channel Grouping Analysis",[]) if e.get("Check")=="Unassigned %"), "0%")
    if float(str(ua_pct).replace("%","") or 0) > 10:
        summary_items.append({"text":f"⚠️ Unassigned traffic: {ua_pct}","ok":None})
    if not summary_items:
        summary_items.append({"text":"✅ GA4 property appears healthy across all checks","ok":True})

    # KPIs
    def g(sec, chk): return next((e.get("Result","—") for e in audit_data.get(sec,[]) if e.get("Check")==chk), "—")
    kpis = [
        ("Time Zone",      g("Property Details","Time Zone")),
        ("Currency",       g("Property Details","Currency")),
        ("Data Retention", g("Property Details","Retention Period")),
        ("Key Events",     g("GA4 Property Limits","Key Events Used")),
        ("LP (not set) %", lp_pct),
        ("Unassigned %",   ua_pct),
    ]

    prop_details = audit_data.get("Property Details",[])
    streams      = audit_data.get("Streams Configuration",[])
    events       = audit_data.get("GA4 Events",[])[:20]
    custom_e     = audit_data.get("Custom Dimensions - Event Scoped",[])
    custom_u     = audit_data.get("Custom Dimensions - User Scoped",[])
    custom_i     = audit_data.get("Custom Dimensions - Item Scoped",[])
    key_events   = audit_data.get("Key Event Details",[])
    lp_data      = audit_data.get("Landing Page Data",[])[:10]
    ch_data      = audit_data.get("Channel Grouping Data",[])

    js_script = f"""
const pptxgen = require("pptxgenjs");
const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";
pres.title = "GA4 Audit – {safe_str(prop_name)}";
pres.author = "Brainlabs GA4 Audit Tool";

// ── Brand tokens ──
const BLK = "0A0A0A", YLW = "FFD426", WHT = "FFFFFF";
const DGR = "1A1A1A", MGR = "3D3D3D", LGR = "9A9A9A";
const GRN = "00C896", RED = "FF4444", ORG = "FF9800", INF = "4A9EFF";
const W = 10, H = 5.625;

const mkShadow = () => ({{ type:"outer", blur:12, offset:3, angle:135, color:"000000", opacity:0.18 }});

// ── Helper: section header ──
function sectionSlide(title, sub) {{
  const s = pres.addSlide();
  s.background = {{ color: BLK }};
  s.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:0.07, h:H, fill:{{ color:YLW }}, line:{{ color:YLW }} }});
  s.addText("BL", {{ x:0.25, y:0.28, w:0.7, h:0.42, fontSize:11, bold:true, color:BLK,
    fill:{{ color:YLW }}, align:"center", valign:"middle", margin:0 }});
  s.addText(title, {{ x:1.15, y:1.9, w:7.8, h:1.0, fontSize:38, bold:true, color:WHT,
    fontFace:"Arial", align:"left", margin:0 }});
  if (sub) s.addText(sub, {{ x:1.15, y:2.98, w:7.8, h:0.5, fontSize:14, color:LGR,
    fontFace:"Arial", align:"left", margin:0 }});
  return s;
}}

// ── Helper: content slide ──
function contentSlide(title) {{
  const s = pres.addSlide();
  s.background = {{ color: DGR }};
  s.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:0.06, h:H, fill:{{ color:YLW }}, line:{{ color:YLW }} }});
  s.addText(title, {{ x:0.25, y:0.22, w:9.5, h:0.55, fontSize:18, bold:true, color:WHT,
    fontFace:"Arial", align:"left", margin:0 }});
  s.addShape(pres.shapes.RECTANGLE, {{ x:0.25, y:0.82, w:9.5, h:0.02, fill:{{ color:MGR }}, line:{{ color:MGR }} }});
  return s;
}}

// ── Helper: add table ──
function addTable(slide, headers, rows, yPos=1.1, colW=null) {{
  const ncols = headers.length;
  const defaultW = (9.5 / ncols);
  const cw = colW || Array(ncols).fill(defaultW);
  const tableData = [];
  // Header row
  tableData.push(headers.map(h => ({{ text:h, options:{{ bold:true, color:WHT, fill:{{ color:"0A0A0A" }}, fontSize:9, align:"left" }} }})));
  rows.forEach((row, ri) => {{
    tableData.push(row.map(cell => ({{
      text: String(cell||"—").substring(0,80),
      options:{{ color: ri%2===0?"FFFFFF":"D0D0D0", fill:{{ color: ri%2===0?"1A1A1A":"222222" }}, fontSize:9, align:"left" }}
    }})));
  }});
  slide.addTable(tableData, {{ x:0.25, y:yPos, w:9.5, colW:cw,
    border:{{ pt:0.5, color:"2A2A2A" }} }});
}}

// ── Helper: KPI card ──
function kpiCard(slide, x, y, label, value, color) {{
  slide.addShape(pres.shapes.RECTANGLE, {{ x, y, w:2.9, h:1.1,
    fill:{{ color:"111111" }}, line:{{ color:"2A2A2A", pt:1 }}, shadow:mkShadow() }});
  slide.addShape(pres.shapes.RECTANGLE, {{ x, y, w:0.05, h:1.1, fill:{{ color }}, line:{{ color }} }});
  slide.addText(label.toUpperCase(), {{ x:x+0.12, y:y+0.08, w:2.7, h:0.25, fontSize:7,
    bold:true, color:LGR, fontFace:"Arial", charSpacing:1.5, margin:0 }});
  slide.addText(String(value), {{ x:x+0.12, y:y+0.35, w:2.7, h:0.65, fontSize:20,
    bold:true, color, fontFace:"Arial", margin:0 }});
}}

// ────────────────────────────────────────────────────────────────────────────
// SLIDE 1 — COVER
// ────────────────────────────────────────────────────────────────────────────
const cover = pres.addSlide();
cover.background = {{ color: BLK }};
cover.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:0.07, h:H, fill:{{ color:YLW }}, line:{{ color:YLW }} }});
cover.addShape(pres.shapes.RECTANGLE, {{ x:0.07, y:3.8, w:W-0.07, h:0.06, fill:{{ color:YLW }}, line:{{ color:YLW }} }});
cover.addText("GA4 AUDIT REPORT", {{ x:0.35, y:1.2, w:9.3, h:0.7, fontSize:11, bold:true, color:YLW,
  fontFace:"Arial", charSpacing:4, align:"left", margin:0 }});
cover.addText("{safe_str(prop_name)}", {{ x:0.35, y:1.95, w:9.3, h:1.1, fontSize:42, bold:true, color:WHT,
  fontFace:"Arial", align:"left", margin:0 }});
cover.addText("{safe_str(date_range)}", {{ x:0.35, y:3.15, w:6, h:0.5, fontSize:14, color:LGR,
  fontFace:"Arial", align:"left", margin:0 }});
cover.addText("BL", {{ x:0.2, y:0.2, w:0.7, h:0.42, fontSize:11, bold:true, color:BLK,
  fill:{{ color:YLW }}, align:"center", valign:"middle", margin:0 }});

// ────────────────────────────────────────────────────────────────────────────
// SLIDE 2 — AUDIT SUMMARY
// ────────────────────────────────────────────────────────────────────────────
const sumSlide = contentSlide("Audit Summary");
const summaryItems = {str(summary_items).replace("True","true").replace("False","false").replace("None","null")};
summaryItems.forEach((item, i) => {{
  const col = item.ok === false ? RED : item.ok === true ? GRN : ORG;
  const y = 1.0 + i * 0.65;
  sumSlide.addShape(pres.shapes.RECTANGLE, {{ x:0.25, y, w:9.5, h:0.55,
    fill:{{ color:"111111" }}, line:{{ color:"2A2A2A", pt:1 }} }});
  sumSlide.addShape(pres.shapes.OVAL, {{ x:0.35, y:y+0.12, w:0.3, h:0.3, fill:{{ color:col }}, line:{{ color:col }} }});
  sumSlide.addText(item.text, {{ x:0.75, y:y+0.09, w:8.8, h:0.35, fontSize:12, color:WHT,
    fontFace:"Arial", margin:0 }});
}});

// ────────────────────────────────────────────────────────────────────────────
// SLIDE 3 — DASHBOARD KPIs
// ────────────────────────────────────────────────────────────────────────────
const kpiSlide = contentSlide("Property Dashboard");
const kpiData = {str([[safe_str(k[0]), safe_str(k[1])] for k in kpis])};
const kpiColors = [INF, YLW, INF, GRN, "{RED if float(str(lp_pct).replace('%','') or 0) > 10 else GRN}", "{RED if float(str(ua_pct).replace('%','') or 0) > 10 else GRN}"];
const kpiPositions = [
  [0.25, 1.0], [3.3, 1.0], [6.55, 1.0],
  [0.25, 2.3], [3.3, 2.3], [6.55, 2.3]
];
kpiData.forEach((kpi, i) => {{
  if (kpiPositions[i]) kpiCard(kpiSlide, kpiPositions[i][0], kpiPositions[i][1], kpi[0], kpi[1], kpiColors[i]);
}});

// ────────────────────────────────────────────────────────────────────────────
// SLIDE 4 — PROPERTY DETAILS & STREAMS
// ────────────────────────────────────────────────────────────────────────────
const propSlide = contentSlide("Property Details & Streams");
const propRows = {str([[safe_str(e.get('Check','')), safe_str(e.get('Result',''))] for e in prop_details])};
const streamRows = {str([[safe_str(e.get('Check','')), safe_str(e.get('Result',''))] for e in streams])};
propSlide.addText("Property Settings", {{ x:0.25, y:0.95, w:4.5, h:0.25, fontSize:9, bold:true, color:YLW, margin:0 }});
addTable(propSlide, ["Check", "Value"], propRows, 1.25, [3.0, 2.5]);
propSlide.addText("Data Streams", {{ x:5.1, y:0.95, w:4.5, h:0.25, fontSize:9, bold:true, color:YLW, margin:0 }});
addTable(propSlide, ["Stream", "Resource Name"], streamRows, 1.25, [2.0, 2.5]);

// ────────────────────────────────────────────────────────────────────────────
// SLIDE 5 — CUSTOM DIMENSIONS
// ────────────────────────────────────────────────────────────────────────────
sectionSlide("Custom Dimensions", "Event · User · Item scoped");

const cdSlide = contentSlide("Custom Dimensions");
const cdEventRows = {str([[safe_str(e.get('Check','')), safe_str(e.get('Result',{}).get('Parameter Name','')), 'Event'] for e in custom_e[:8]])};
const cdUserRows  = {str([[safe_str(e.get('Check','')), safe_str(e.get('Result',{}).get('Parameter Name','')), 'User'] for e in custom_u[:8]])};
const cdItemRows  = {str([[safe_str(e.get('Check','')), safe_str(e.get('Result',{}).get('Parameter Name','')), 'Item'] for e in custom_i[:8]])};
const allCdRows   = [...cdEventRows, ...cdUserRows, ...cdItemRows];
addTable(cdSlide, ["Display Name", "Parameter Name", "Scope"], allCdRows, 1.0, [3.5, 3.5, 2.0]);

// ────────────────────────────────────────────────────────────────────────────
// SLIDE 6 — EVENT INVENTORY
// ────────────────────────────────────────────────────────────────────────────
sectionSlide("Events & Key Events", "What's being tracked");
const evSlide = contentSlide("Event Inventory (Top 20)");
const evRows = {str([[safe_str(e.get('Check','')), safe_str(e.get('Result',''))] for e in events])};
addTable(evSlide, ["Event Name", "Event Count"], evRows, 1.0, [5.5, 3.5]);

const keRows = {str([[safe_str(e.get('Check','')), safe_str(e.get('Result',{}).get('Create Time','')), safe_str(e.get('Result',{}).get('Counting Method',''))] for e in key_events[:12]])};
if (keRows.length > 0) {{
  const keSlide = contentSlide("Key Events (Conversions)");
  addTable(keSlide, ["Event Name", "Created", "Counting Method"], keRows, 1.0, [4.0, 2.5, 2.5]);
}}

// ────────────────────────────────────────────────────────────────────────────
// SLIDE 7 — TRAFFIC QUALITY
// ────────────────────────────────────────────────────────────────────────────
sectionSlide("Traffic Quality", "Landing pages · Channel grouping · Unassigned");
const lpRows = {str([[safe_str(e.get('Landing Page','')), str(e.get('Sessions',''))] for e in lp_data])};
const chRows = {str([[safe_str(e.get('Channel Group','')), str(e.get('Sessions',''))] for e in ch_data])};
if (lpRows.length > 0) {{
  const lpSlide = contentSlide("Landing Page Data (Top 10)");
  addTable(lpSlide, ["Landing Page", "Sessions"], lpRows, 1.0, [6.5, 2.5]);
}}
if (chRows.length > 0) {{
  const chSlide = contentSlide("Channel Grouping");
  addTable(chSlide, ["Channel Group", "Sessions"], chRows, 1.0, [6.5, 2.5]);
}}

// ────────────────────────────────────────────────────────────────────────────
// LAST SLIDE — END CARD
// ────────────────────────────────────────────────────────────────────────────
const end = pres.addSlide();
end.background = {{ color: BLK }};
end.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:W, h:0.07, fill:{{ color:YLW }}, line:{{ color:YLW }} }});
end.addShape(pres.shapes.RECTANGLE, {{ x:0, y:H-0.07, w:W, h:0.07, fill:{{ color:YLW }}, line:{{ color:YLW }} }});
end.addText("BL", {{ x:4.65, y:1.6, w:0.7, h:0.42, fontSize:11, bold:true, color:BLK,
  fill:{{ color:YLW }}, align:"center", valign:"middle", margin:0 }});
end.addText("Brainlabs", {{ x:3.5, y:2.15, w:3, h:0.6, fontSize:24, bold:true, color:WHT,
  fontFace:"Arial", align:"center", margin:0 }});
end.addText("GA4 Audit Tool", {{ x:3.2, y:2.78, w:3.6, h:0.4, fontSize:13, color:LGR,
  fontFace:"Arial", align:"center", margin:0 }});

pres.writeFile({{ fileName: "output.pptx" }}).then(() => process.exit(0)).catch(e => {{ console.error(e); process.exit(1); }});
"""

    with tempfile.TemporaryDirectory() as tmpdir:
        js_path  = os.path.join(tmpdir, "gen.js")
        out_path = os.path.join(tmpdir, "output.pptx")
        with open(js_path, "w") as f:
            f.write(js_script)
        result = subprocess.run(
            ["node", "-e", f"process.chdir('{tmpdir}'); " + open(js_path).read()],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            return {"success": False, "error": result.stderr[:500]}
        with open(out_path, "rb") as f:
            pptx_bytes = f.read()

    filename = f"GA4_Audit_{prop_name.replace(' ','_')}.pptx"
    return StreamingResponse(
        io.BytesIO(pptx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )