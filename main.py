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

        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[{"start_date": start_date, "end_date": end_date}],
            limit=limit,
            order_bys=order_bys,
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