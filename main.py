from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from ga4audit import run_ga4_audit
import uvicorn

app = FastAPI()

# Allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in production, specify your frontend URL
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/run-audit")
def run_audit(property_id: str, start_date: str = "30daysAgo", end_date: str = "today"):
    try:
        result = run_ga4_audit(property_id, start_date, end_date)
        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
