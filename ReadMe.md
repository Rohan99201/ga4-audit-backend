# 📊 GA4 Audit Automation Tool

A complete **Google Analytics 4 Audit Tool** that automatically checks:

* Property settings
* Streams
* Events
* PII issues
* Duplicate transactions
* Missing item data
* GA4 limits
* Retention settings
* Ecommerce validation

Built using **Python + FastAPI + GA4 Admin & Data APIs**.

---

# 🚀 What This Tool Does

This audit script connects to your GA4 property and generates a report containing:

### 🔧 Property Checks

* Property name
* Timezone
* Currency
* Data retention period
* Streams available

### 📈 Ecommerce Checks

* Duplicate transaction IDs
* Missing transaction IDs
* Revenue vs item mismatch
* Items with null name but revenue

### 🔐 PII Checks

Scans URLs for:

* email
* phone
* personal data

### 📊 Limits

* Custom dimensions
* Custom metrics
* Audiences
* Key events

---

# 🧰 Prerequisites

Before running the audit, you must set up a **Google Service Account**.

---

# 🪪 Step 1 : Create Google Cloud Project

Go to:
[https://console.cloud.google.com/](https://console.cloud.google.com/)

Create a new project.

---

# 🔌 Step 2 : Enable APIs

Enable BOTH:

* Google Analytics Admin API
* Google Analytics Data API

Links:

```
https://console.cloud.google.com/apis/library/analyticsadmin.googleapis.com
https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com
```

---

# 🤖 Step 3 : Create Service Account

Go to:

```
IAM & Admin → Service Accounts
```

Create service account:

```
Name: ga4-audit
```

No special role needed in GCP.

Click:

```
Keys → Add Key → JSON
```

Download the JSON file.

⚠️ Do NOT upload this file to GitHub.

---

# 👤 Step 4 : Add Service Account to GA4

Go to your GA4 property:

```
Admin → Property Access Management
```

Add this email:

```
YOUR_SERVICE_ACCOUNT@project.iam.gserviceaccount.com
```

Role:

```
Viewer (minimum)
Editor (recommended)
```

Without this step, the audit will fail.

---

# 💻 Local Setup

## 1. Clone repo

```bash
git clone https://github.com/YOUR_USERNAME/ga4-audit.git
cd backend
```

---

## 2. Create virtual environment

```bash
python -m venv venv
venv\Scripts\activate   (Windows)
```

---

## 3. Install packages

```bash
pip install fastapi uvicorn google-analytics-admin google-analytics-data python-dotenv pandas
```

---

## 4. Add credentials to `.env`

Create `.env` inside backend folder.

Paste your full JSON:

```
SERVICE_ACCOUNT_JSON={PASTE_FULL_JSON}
```

⚠️ Entire JSON must be in ONE line.

---

# ▶️ Run Backend Locally

```bash
uvicorn main:app --reload
```

Open:

```
http://127.0.0.1:8000
```

Test API:

```
http://127.0.0.1:8000/run-audit?property_id=123456789
```

With date range:

```
http://127.0.0.1:8000/run-audit?property_id=123456789&start_date=2025-01-01&end_date=2025-02-01
```

---

# 🌍 Deploy Backend (Render)

### Push to GitHub

Make sure `.env` is in `.gitignore`.

```
git add .
git commit -m "initial"
git push
```

---

### Create Render Web Service

Go to:
[https://render.com](https://render.com)

Create **Web Service**

Settings:

```
Build command: pip install -r requirements.txt
Start command: uvicorn main:app --host 0.0.0.0 --port 10000
```

---

### Add Environment Variable in Render

```
SERVICE_ACCOUNT_JSON = {paste full json}
```

Deploy.

---

# 🖥️ Frontend Setup (Optional)

Frontend can call API:

```
https://your-render-url/run-audit?property_id=XXXX
```

Deploy frontend on **Vercel**.

---

# 📂 Project Structure

```
backend/
 ├── main.py
 ├── ga4audit.py
 ├── requirements.txt
 ├── .env
```

---

# 🧪 Example Output

```
Duplicate Transaction Count: 2
Missing Item Names: 3
PII Issues: None
Streams: Web + Android
Retention: 14 months
```

---

# ❗ Common Errors

### SERVICE_ACCOUNT_JSON not set

Add to `.env`.

### 403 Permission error

Service account not added to GA4 property.

### Property not found

Check property ID is numeric only.

---

# 🔮 Future Improvements

* PDF report export
* Google Sheets export
* Multi property audit
* Slack alerts
* BigQuery audit

---

# 👨‍💻 Author : Rohan 

Built for GA4 debugging, ecommerce QA, and analytics validation.
