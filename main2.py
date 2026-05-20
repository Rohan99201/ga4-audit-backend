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
    Generates a Brainlabs-branded GA4 audit .pptx using pure python-pptx.
    POST body: { "data": <audit_data>, "property_name": "...", "date_range": "..." }
    """
    import io
    from pptx import Presentation
    from pptx.util import Inches, Pt, Emu
    from pptx.dml.color import RGBColor
    from pptx.enum.text import PP_ALIGN
    from pptx.util import Inches, Pt
    from fastapi.responses import StreamingResponse

    get_user_credentials(request)

    if not body:
        return {"success": False, "error": "Request body required."}

    audit_data = body.get("data", {})
    prop_name  = body.get("property_name", "GA4 Property")
    date_range = body.get("date_range", "30daysAgo – today")

    # ── Brainlabs colours ──────────────────────────────────────────────────
    C_BLACK  = RGBColor(0x0A, 0x0A, 0x0A)
    C_YELLOW = RGBColor(0xFF, 0xD4, 0x26)
    C_WHITE  = RGBColor(0xFF, 0xFF, 0xFF)
    C_DGREY  = RGBColor(0x1A, 0x1A, 0x1A)
    C_MGREY  = RGBColor(0x3D, 0x3D, 0x3D)
    C_LGREY  = RGBColor(0x9A, 0x9A, 0x9A)
    C_GREEN  = RGBColor(0x00, 0xC8, 0x96)
    C_RED    = RGBColor(0xFF, 0x44, 0x44)
    C_ORANGE = RGBColor(0xFF, 0x98, 0x00)
    C_BLUE   = RGBColor(0x4A, 0x9E, 0xFF)

    SW = Inches(10)   # slide width
    SH = Inches(5.625)  # slide height

    prs = Presentation()
    prs.slide_width  = SW
    prs.slide_height = SH

    blank_layout = prs.slide_layouts[6]  # completely blank

    def rgb_hex(r):
        return RGBColor((r>>16)&0xFF, (r>>8)&0xFF, r&0xFF)

    def add_rect(slide, x, y, w, h, fill_color, line_color=None):
        shape = slide.shapes.add_shape(1, Inches(x), Inches(y), Inches(w), Inches(h))
        shape.fill.solid()
        shape.fill.fore_color.rgb = fill_color
        if line_color:
            shape.line.color.rgb = line_color
            shape.line.width = Pt(0.5)
        else:
            shape.line.fill.background()
        return shape

    def add_text_box(slide, text, x, y, w, h, size=12, bold=False, color=None,
                     align=PP_ALIGN.LEFT, font_name="Arial", italic=False):
        txb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
        tf  = txb.text_frame
        tf.word_wrap = False
        p   = tf.paragraphs[0]
        p.alignment = align
        run = p.add_run()
        run.text = str(text)[:120]
        run.font.name = font_name
        run.font.size = Pt(size)
        run.font.bold = bold
        run.font.italic = italic
        if color:
            run.font.color.rgb = color
        return txb

    def brand_bar(slide):
        """Left yellow accent bar + BL mark."""
        add_rect(slide, 0, 0, 0.07, 5.625, C_YELLOW)
        box = add_rect(slide, 0.18, 0.22, 0.6, 0.36, C_YELLOW)
        add_text_box(slide, "BL", 0.18, 0.22, 0.6, 0.36, size=9, bold=True,
                     color=C_BLACK, align=PP_ALIGN.CENTER)

    def slide_title(slide, title, y=0.2):
        add_text_box(slide, title, 0.25, y, 9.5, 0.5, size=16, bold=True, color=C_WHITE)
        add_rect(slide, 0.25, y+0.55, 9.5, 0.02, C_MGREY)

    def section_divider(title, subtitle=""):
        s = prs.slides.add_slide(blank_layout)
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = C_BLACK
        add_rect(s, 0, 0, 0.07, 5.625, C_YELLOW)
        add_text_box(s, "BL", 0.18, 0.22, 0.6, 0.36, size=9, bold=True, color=C_BLACK, align=PP_ALIGN.CENTER)
        add_text_box(s, title,    1.1, 1.8, 8.0, 1.0, size=34, bold=True, color=C_WHITE)
        if subtitle:
            add_text_box(s, subtitle, 1.1, 2.9, 8.0, 0.5, size=13, color=C_LGREY)
        return s

    def content_slide(title):
        s = prs.slides.add_slide(blank_layout)
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = C_DGREY
        brand_bar(s)
        slide_title(s, title)
        return s

    def add_check_result_table(slide, entries, y_start=0.9, max_rows=14):
        rows = entries[:max_rows]
        ncols = 2
        col_w = [5.0, 4.5]
        # header
        for ci, (label, w) in enumerate(zip(["Check", "Result"], col_w)):
            x = 0.25 + sum(col_w[:ci])
            add_rect(slide, x, y_start, w, 0.28, C_BLACK)
            add_text_box(slide, label, x+0.05, y_start+0.03, w-0.1, 0.22,
                         size=8, bold=True, color=C_YELLOW)
        # rows
        for ri, e in enumerate(rows):
            y = y_start + 0.28 + ri * 0.27
            bg = C_DGREY if ri % 2 == 0 else RGBColor(0x22, 0x22, 0x22)
            check  = str(e.get("Check", ""))[:60]
            result = e.get("Result", "")
            if isinstance(result, (list, dict)):
                result = str(result)[:80]
            result = str(result)[:80]
            rc = C_GREEN if "✅" in result else C_RED if "❌" in result else C_LGREY
            for ci, (val, w, vc) in enumerate(zip([check, result], col_w, [C_WHITE, rc])):
                x = 0.25 + sum(col_w[:ci])
                add_rect(slide, x, y, w, 0.25, bg)
                add_text_box(slide, val, x+0.05, y+0.03, w-0.1, 0.20, size=8, color=vc)

    def add_simple_table(slide, headers, rows, y_start=0.9, max_rows=14):
        rows = rows[:max_rows]
        ncols = len(headers)
        col_w = [9.5 / ncols] * ncols
        # header
        for ci, (h, w) in enumerate(zip(headers, col_w)):
            x = 0.25 + ci * w
            add_rect(slide, x, y_start, w, 0.28, C_BLACK)
            add_text_box(slide, h, x+0.05, y_start+0.03, w-0.1, 0.22, size=8, bold=True, color=C_YELLOW)
        # rows
        for ri, row in enumerate(rows):
            y = y_start + 0.28 + ri * 0.27
            bg = C_DGREY if ri % 2 == 0 else RGBColor(0x22, 0x22, 0x22)
            for ci, (val, w) in enumerate(zip(row, col_w)):
                x = 0.25 + ci * w
                add_rect(slide, x, y, w, 0.25, bg)
                add_text_box(slide, str(val)[:60], x+0.05, y+0.03, w-0.1, 0.20, size=8, color=C_WHITE)

    # ── Build summary data ─────────────────────────────────────────────────
    summary_items = []
    if audit_data.get("Duplicate Transactions"):
        summary_items.append(("❌ Duplicate transactions detected", C_RED))
    if audit_data.get("Transaction Where Item Data Missing"):
        summary_items.append(("❌ Purchase events with missing item names", C_RED))
    if any(str(e.get("Result","")).startswith("❌") for e in audit_data.get("PII Check",[])):
        summary_items.append(("❌ Potential PII detected in page paths", C_RED))
    lp_pct_raw = next((e.get("Result","0%") for e in audit_data.get("Landing Page Analysis",[]) if e.get("Check")=="Landing Page (not set) %"), "0%")
    ua_pct_raw = next((e.get("Result","0%") for e in audit_data.get("Channel Grouping Analysis",[]) if e.get("Check")=="Unassigned %"), "0%")
    try:
        if float(str(lp_pct_raw).replace("%","")) > 10:
            summary_items.append((f"⚠️ Landing Page (not set) rate: {lp_pct_raw}", C_ORANGE))
    except: pass
    try:
        if float(str(ua_pct_raw).replace("%","")) > 10:
            summary_items.append((f"⚠️ Unassigned traffic: {ua_pct_raw}", C_ORANGE))
    except: pass
    if not summary_items:
        summary_items.append(("✅ GA4 property appears healthy across all checks", C_GREEN))

    def g(sec, chk):
        return next((str(e.get("Result","—")) for e in audit_data.get(sec,[]) if e.get("Check")==chk), "—")

    kpis = [
        ("Time Zone",      g("Property Details","Time Zone"),      C_BLUE),
        ("Currency",       g("Property Details","Currency"),       C_YELLOW),
        ("Retention",      g("Property Details","Retention Period"),C_BLUE),
        ("Key Events",     g("GA4 Property Limits","Key Events Used"), C_GREEN),
        ("LP (not set) %", str(lp_pct_raw), C_RED if float(str(lp_pct_raw).replace("%","") or 0)>10 else C_GREEN),
        ("Unassigned %",   str(ua_pct_raw), C_RED if float(str(ua_pct_raw).replace("%","") or 0)>10 else C_GREEN),
    ]

    # ── SLIDE 1: Cover ─────────────────────────────────────────────────────
    cover = prs.slides.add_slide(blank_layout)
    cover.background.fill.solid()
    cover.background.fill.fore_color.rgb = C_BLACK
    add_rect(cover, 0, 0, 0.07, 5.625, C_YELLOW)
    add_rect(cover, 0.07, 3.8, 9.93, 0.06, C_YELLOW)
    add_text_box(cover, "BL", 0.18, 0.22, 0.6, 0.36, size=9, bold=True, color=C_BLACK, align=PP_ALIGN.CENTER)
    add_text_box(cover, "GA4 AUDIT REPORT", 0.35, 1.2, 9.0, 0.55, size=10, bold=True, color=C_YELLOW)
    add_text_box(cover, prop_name[:60],      0.35, 1.9, 9.0, 1.0,  size=36, bold=True, color=C_WHITE)
    add_text_box(cover, date_range,          0.35, 3.1, 6.0, 0.5,  size=13, color=C_LGREY)

    # ── SLIDE 2: Audit Summary ─────────────────────────────────────────────
    sum_slide = content_slide("Audit Summary")
    for i, (text, col) in enumerate(summary_items[:7]):
        y = 1.0 + i * 0.62
        add_rect(sum_slide, 0.25, y, 9.5, 0.52, RGBColor(0x11,0x11,0x11))
        add_rect(sum_slide, 0.25, y, 0.05, 0.52, col)
        add_text_box(sum_slide, text[:100], 0.45, y+0.1, 9.0, 0.35, size=11, color=C_WHITE)

    # ── SLIDE 3: KPI Dashboard ─────────────────────────────────────────────
    kpi_slide = content_slide("Property Dashboard")
    positions = [(0.25,1.0),(3.55,1.0),(6.85,1.0),(0.25,2.7),(3.55,2.7),(6.85,2.7)]
    for i, (label, value, col) in enumerate(kpis):
        if i >= len(positions): break
        px, py = positions[i]
        add_rect(kpi_slide, px, py, 3.0, 1.3, RGBColor(0x11,0x11,0x11))
        add_rect(kpi_slide, px, py, 0.05, 1.3, col)
        add_text_box(kpi_slide, label.upper(), px+0.12, py+0.08, 2.75, 0.22, size=7, bold=True, color=C_LGREY)
        add_text_box(kpi_slide, str(value)[:25], px+0.12, py+0.38, 2.75, 0.6, size=19, bold=True, color=col)

    # ── SLIDE 4: Property Details ──────────────────────────────────────────
    section_divider("Property Details", "Settings · Streams · Limits")
    prop_s = content_slide("Property Details & Streams")
    add_check_result_table(prop_s, audit_data.get("Property Details",[]), y_start=0.9, max_rows=10)

    streams = audit_data.get("Streams Configuration",[])
    if streams:
        st_s = content_slide("Streams Configuration")
        add_check_result_table(st_s, streams, y_start=0.9, max_rows=10)

    limits = audit_data.get("GA4 Property Limits",[])
    if limits:
        lim_s = content_slide("GA4 Property Limits")
        add_check_result_table(lim_s, limits, y_start=0.9, max_rows=10)

    # ── SLIDE 5: Custom Dimensions ─────────────────────────────────────────
    section_divider("Custom Dimensions", "Event · User · Item scoped")
    for scope in ["Event Scoped","User Scoped","Item Scoped"]:
        dims = audit_data.get(f"Custom Dimensions - {scope}",[])
        if dims:
            ds = content_slide(f"Custom Dimensions — {scope}")
            rows = [(e.get("Check",""), e.get("Result",{}).get("Parameter Name",""), scope.split()[0]) for e in dims]
            add_simple_table(ds, ["Display Name","Parameter Name","Scope"], rows, y_start=0.9, max_rows=14)

    # ── SLIDE 6: Event Inventory ───────────────────────────────────────────
    section_divider("Events & Key Events", "What's being tracked")
    events = audit_data.get("GA4 Events",[])
    if events:
        ev_s = content_slide("Event Inventory (Top 20)")
        rows = [(e.get("Check",""), e.get("Result","")) for e in events[:20]]
        add_simple_table(ev_s, ["Event Name","Event Count"], rows, y_start=0.9, max_rows=14)

    key_events = audit_data.get("Key Event Details",[])
    if key_events:
        ke_s = content_slide("Key Events (Conversions)")
        rows = [(e.get("Check",""), e.get("Result",{}).get("Create Time",""), e.get("Result",{}).get("Counting Method","")) for e in key_events[:12]]
        add_simple_table(ke_s, ["Event Name","Created","Counting Method"], rows, y_start=0.9, max_rows=12)

    # ── SLIDE 7: PII Check ─────────────────────────────────────────────────
    pii = audit_data.get("PII Check",[])
    if pii:
        section_divider("PII & Data Quality", "")
        pii_s = content_slide("PII Check")
        add_check_result_table(pii_s, pii, y_start=0.9, max_rows=12)

    # ── SLIDE 8: Transactions ──────────────────────────────────────────────
    txns = audit_data.get("Transactions",[])
    if txns:
        section_divider("E-commerce & Transactions", "")
        tx_s = content_slide("Transaction Health")
        add_check_result_table(tx_s, txns, y_start=0.9, max_rows=12)

    dup = audit_data.get("Duplicate Transactions",[])
    if dup:
        dup_s = content_slide("Duplicate Transactions")
        rows = [(e.get("transactionId",""), e.get("count","")) for e in dup[:14]]
        add_simple_table(dup_s, ["Transaction ID","Count"], rows, y_start=0.9, max_rows=14)

    # ── SLIDE 9: Traffic Quality ───────────────────────────────────────────
    section_divider("Traffic Quality", "Landing pages · Channel grouping")
    lp_anal = audit_data.get("Landing Page Analysis",[])
    if lp_anal:
        lpa_s = content_slide("Landing Page Analysis")
        add_check_result_table(lpa_s, lp_anal, y_start=0.9, max_rows=8)

    lp_data = audit_data.get("Landing Page Data",[])
    if lp_data:
        lpd_s = content_slide("Landing Page Data (Top 14)")
        rows = [(e.get("Landing Page","")[:50], e.get("Sessions","")) for e in lp_data[:14]]
        add_simple_table(lpd_s, ["Landing Page","Sessions"], rows, y_start=0.9, max_rows=14)

    ch_anal = audit_data.get("Channel Grouping Analysis",[])
    if ch_anal:
        cha_s = content_slide("Channel Grouping Analysis")
        add_check_result_table(cha_s, ch_anal, y_start=0.9, max_rows=8)

    ch_data = audit_data.get("Channel Grouping Data",[])
    if ch_data:
        chd_s = content_slide("Channel Grouping Data")
        rows = [(e.get("Channel Group",""), e.get("Sessions","")) for e in ch_data[:14]]
        add_simple_table(chd_s, ["Channel Group","Sessions"], rows, y_start=0.9, max_rows=14)

    ua_data = audit_data.get("Unassigned Source/Medium Data",[])
    if ua_data:
        ua_s = content_slide("Unassigned Source/Medium")
        rows = [(e.get("Source",""), e.get("Medium",""), e.get("Sessions","")) for e in ua_data[:14]]
        add_simple_table(ua_s, ["Source","Medium","Sessions"], rows, y_start=0.9, max_rows=14)

    # ── END CARD ───────────────────────────────────────────────────────────
    end = prs.slides.add_slide(blank_layout)
    end.background.fill.solid()
    end.background.fill.fore_color.rgb = C_BLACK
    add_rect(end, 0, 0,      SW.inches, 0.07, C_YELLOW)
    add_rect(end, 0, 5.555,  SW.inches, 0.07, C_YELLOW)
    add_text_box(end, "BL",            4.65, 1.7,  0.7, 0.38, size=9, bold=True, color=C_BLACK, align=PP_ALIGN.CENTER)
    add_text_box(end, "Brainlabs",     3.5,  2.2,  3.0, 0.55, size=22, bold=True, color=C_WHITE, align=PP_ALIGN.CENTER)
    add_text_box(end, "GA4 Audit Tool",3.2,  2.82, 3.6, 0.4,  size=12, color=C_LGREY, align=PP_ALIGN.CENTER)

    # ── Stream to response ─────────────────────────────────────────────────
    buf = io.BytesIO()
    prs.save(buf)
    buf.seek(0)
    filename = f"GA4_Audit_{prop_name.replace(' ','_')}.pptx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )