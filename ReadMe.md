1. Open your browser or use a tool like curl or Postman, and go to:

https://ga4-audit-backend.onrender.com/

2. You should see a JSON response like:

{
  "message": "GA4 Audit API is running 🚀"
}

If this works, your API is live!

3. Test /run-audit endpoint
Use the following format in your browser or Postman:

https://ga4-audit-backend.onrender.com/run-audit?property_id=490419193&start_date=2025-04-01&end_date=2025-06-25
https://ga4-audit-backend.onrender.com/run-audit?property_id=343819188&start_date=2025-06-01&end_date=2025-06-30