# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Streamlit-based Amazon Analytics dashboard application for processing Amazon Seller Central transaction reports.

## Setup

```bash
pip install -r requirements.txt
```

## Running the App

```bash
cd C:/Users/vuser/Documents/Projects/amazon-streamlit
streamlit run streamlit_app.py
```

## Project Structure

- `streamlit_app.py` - Main Streamlit application entry point
- `app.py` - Basic Streamlit app (placeholder)
- `requirements.txt` - Python dependencies

## Architecture

The Streamlit app reuses core processing logic from the Amazon project at:
`C:/Users/vuser/Documents/Projects/Amazon/src/`

Key processing modules (imported from Amazon project):
- `processor/data_processing.py` - Data processing logic
- `processor/google_sheets.py` - Google Sheets integration
- `utils/auth_utils.py` - Google OAuth authentication

## Features

1. Upload Amazon Seller Central tab-separated TXT report
2. Select date range based on file contents
3. Generate summary pivot tables by month
4. Process QTY and Order data
5. Merge and map SKUs via Google Sheets
6. Calculate product costs from landed_cost and pdb_us sheets
7. Download processed data as Excel with multiple sheets

## Google Sheets Authentication

This app uses **Service Account** for Google Sheets access (not OAuth user login).

### Local Development Setup

1. **Create Service Account in Google Cloud Console:**
   - Go to https://console.cloud.google.com/
   - Create a new project or select existing
   - Navigate to **IAM & Admin > Service Accounts**
   - Create a service account
   - Download the JSON key file as `service_account.json`

2. **Share Google Sheets with the Service Account:**
   - Copy the service account email (e.g., `your-service@project.iam.gserviceaccount.com`)
   - Share all required sheets (`landed_cost`, `pdb_us`, `SKU Manual Mapping`) with this email

3. **Place the key file:**
   - Put `service_account.json` in the `amazon-streamlit` directory
   - Or in the `Amazon` project directory as fallback

4. **GitHub safety:**
   - `service_account.json` is excluded from git via `.gitignore`
   - An `example_service_account.json` template is provided for reference

### Streamlit Cloud Deployment

1. **Push code to GitHub** (without `service_account.json`)

2. **Add Secrets in Streamlit Cloud dashboard:**
   - Go to your app → **Settings** → **Secrets**
   - Add the following, pasting your full `service_account.json` content:
   ```toml
   gcp_service_account = '''
   {
     "type": "service_account",
     "project_id": "your-project-id",
     "private_key_id": "...",
     "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n",
     "client_email": "xxx@xxx.iam.gserviceaccount.com",
     ...
   }
   '''
   ```

3. **Deploy** - The app will read credentials from `st.secrets` on Streamlit Cloud
