from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
from starlette.middleware.sessions import SessionMiddleware
from google.oauth2.credentials import Credentials
from google.analytics.admin import AnalyticsAdminServiceClient
# from googleapiclient.discovery import build
import requests
import os
import json
from ga4audit import run_ga4_audit_with_creds

app = FastAPI()

# ── Environment Variables ──────────────────────────────────────────────────
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
REDIRECT_URI         = os.getenv("REDIRECT_URI", "https://ga4-audit-backend.onrender.com/auth/callback")
FRONTEND_URL         = os.getenv("FRONTEND_URL", "https://your-frontend.onrender.com")
SECRET_KEY           = os.getenv("SECRET_KEY", "change-this-to-a-random-secret")

GOOGLE_AUTH_URL  = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/analytics.edit",
    "openid",
    "email",
    "profile",
]

# ── Middleware ─────────────────────────────────────────────────────────────
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, max_age=3600)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Helper: build Credentials from session ─────────────────────────────────
def get_user_credentials(request: Request) -> Credentials:
    token_data = request.session.get("token_data")
    if not token_data:
        raise HTTPException(status_code=401, detail="Not authenticated. Please log in.")
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
    """Step 1 — redirect user to Google's OAuth consent screen."""
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
def auth_callback(request: Request, code: str = Query(...)):
    """Step 2 — Google redirects back here with an auth code. Exchange it for tokens."""
    token_response = requests.post(GOOGLE_TOKEN_URL, data={
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    })

    if token_response.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {token_response.text}")

    token_data = token_response.json()
    request.session["token_data"] = token_data

    # Fetch basic user info and store it too
    userinfo_resp = requests.get(
        "https://www.googleapis.com/oauth2/v3/userinfo",
        headers={"Authorization": f"Bearer {token_data['access_token']}"}
    )
    if userinfo_resp.status_code == 200:
        request.session["user_info"] = userinfo_resp.json()

    # Redirect to frontend after successful login
    return RedirectResponse(url=f"{FRONTEND_URL}?auth=success")


@app.get("/auth/me")
def auth_me(request: Request):
    """Returns logged-in user info."""
    user_info = request.session.get("user_info")
    if not user_info:
        return JSONResponse(status_code=401, content={"authenticated": False})
    return {"authenticated": True, "user": user_info}


@app.get("/auth/logout")
def auth_logout(request: Request):
    """Clears the session."""
    request.session.clear()
    return {"message": "Logged out successfully."}


@app.get("/list-properties")
def list_properties(request: Request):
    """Returns all GA4 properties the logged-in user has access to."""
    creds = get_user_credentials(request)
    try:
        admin_client = AnalyticsAdminServiceClient(credentials=creds)
        accounts = admin_client.list_account_summaries()

        properties = []
        for account in accounts:
            for prop_summary in account.property_summaries:
                properties.append({
                    "property_id": prop_summary.property.replace("properties/", ""),
                    "display_name": prop_summary.display_name,
                    "account_name": account.display_name,
                    "property_resource": prop_summary.property,
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