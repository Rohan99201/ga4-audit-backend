from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from ga4audit import run_ga4_audit

app = FastAPI()

# Allow frontend to connect (CORS policy)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace "*" with your frontend domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "GA4 Audit API is running 🚀"}

@app.get("/run-audit")
def run_audit(
    property_id: str = Query(...),
    start_date: str = Query("30daysAgo"),
    end_date: str = Query("today")
):
    try:
        results = run_ga4_audit(property_id, start_date=start_date, end_date=end_date)
        return {"success": True, "data": results}
    except Exception as e:
        return {"success": False, "error": str(e)}
