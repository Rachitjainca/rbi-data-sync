# RBI Excel Data to Google Sheets Automation

Automatic daily fetching of RBI Excel data with hourly GitHub Actions scheduler, extracting all sheets and creating new worksheets monthly in a single Google Sheet.

## Features

✅ **Hourly Scheduling**: Runs every hour to catch RBI data updates regardless of time  
✅ **Multi-Sheet Support**: Automatically creates new worksheets for each sheet in the RBI Excel file  
✅ **Monthly Organization**: Sheet names from source Excel are preserved  
✅ **Smart Updates**: Date-based upsert logic prevents duplicates; updates existing rows or appends new ones  
✅ **Single Google Sheet**: All data consolidated into one file with multiple worksheets  
✅ **Error Handling**: Comprehensive logging and error recovery  
✅ **GitHub Actions**: Fully automated via GitHub workflow  

## Prerequisites

1. **Python 3.11+** (for local testing)
2. **Google Cloud Project** with Sheets API enabled
3. **Google Service Account** with JSON credentials
4. **GitHub Repository** with Secrets configured
5. **Google Sheet** already created in your Drive

## Setup Instructions

### Step 1: Create Google Cloud Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create or select a project
3. Enable **Google Sheets API**:
   - Go to **APIs & Services** → **Library**
   - Search for "Google Sheets API" and click **Enable**
4. Create a **Service Account**:
   - Go to **APIs & Services** → **Credentials**
   - Click **Create Credentials** → **Service Account**
   - Fill in the service account details (name, description optional)
   - Click **Create and Continue**
   - Skip optional steps, click **Done**
5. **Generate JSON Key**:
   - Click on the created service account
   - Go to **Keys** tab
   - Click **Add Key** → **Create new key** → **JSON**
   - Save the JSON file locally (keep it secure!)

### Step 2: Create Google Sheet

1. Go to [Google Sheets](https://sheets.google.com)
2. Create a new spreadsheet (name it something like "RBI Data Sync")
3. Share the sheet with the **Service Account email** (use the `client_email` from the JSON key file):
   - Click **Share** button
   - Paste the service account email
   - Grant **Editor** access
   - Click **Share**
4. Note the **Spreadsheet ID** from the URL:
   ```
   https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit
   ```

### Step 3: Configure GitHub Secrets

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret** and add:
   - **Name**: `GOOGLE_SERVICE_ACCOUNT_JSON`  
   **Value**: Entire contents of the JSON key file (copy the full JSON)
   
   - **Name**: `GOOGLE_SPREADSHEET_ID`  
   **Value**: Your spreadsheet ID from Step 2

### Step 4: Test Locally (Optional)

1. Clone or navigate to your repository
2. Create a `.env` file (for local testing only):
   ```
   GOOGLE_SERVICE_ACCOUNT_JSON=/path/to/service-account-key.json
   GOOGLE_SPREADSHEET_ID=your_spreadsheet_id
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the script:
   ```bash
   python fetch_and_update.py
   ```

### Step 5: Enable GitHub Actions

1. Go to your repository
2. Navigate to **Actions** tab
3. Click **I understand my workflows, go ahead and enable them**
4. The workflow will run automatically on the schedule (every hour)

## How It Works

1. **Hourly Trigger**: GitHub Actions runs the workflow at the top of every hour (UTC)
2. **Download**: Fetches the latest RBI Excel file from the URL
3. **Parse**: Extracts all sheet names and data from the Excel file
4. **Create/Update**: For each sheet in the Excel file:
   - Checks if a worksheet with that name exists in the Google Sheet
   - Creates a new worksheet if it doesn't exist
   - Updates or inserts data based on date identifier
5. **Cleanup**: Removes the downloaded file
6. **Logging**: All activities logged to `rbi_data_sync.log`

## File Structure

```
Payments/
├── fetch_and_update.py          # Main script
├── requirements.txt              # Python dependencies
├── README.md                      # This file
├── .gitignore                     # Git ignore rules
├── .github/
│   └── workflows/
│       └── fetch-data.yml        # GitHub Actions workflow
└── rbi_data_sync.log             # Log file (generated at runtime)
```

## Monitoring & Troubleshooting

### View GitHub Actions Logs

1. Go to your repository
2. Click **Actions** tab
3. Click on the workflow run to see detailed logs
4. Scroll down to see execution details and any errors

### Common Issues

| Issue | Solution |
|-------|----------|
| **Authentication Failed** | Verify Service Account JSON in `GOOGLE_SERVICE_ACCOUNT_JSON` secret is valid and complete |
| **Sheet Not Found** | Ensure Google Sheet is shared with the Service Account email (check `client_email` in JSON key) |
| **API Quota Exceeded** | Reduce frequency or batch operations; check Google Sheets API quotas |
| **Download Timeout** | RBI file may be temporarily unavailable; script will retry next hour |
| **Empty Data** | Verify the RBI URL is still valid and returns data |

### Enable Debug Logging

To get more detailed logs, modify the workflow to set debug:
```yaml
env:
  DEBUG: "true"
```

## Data Schema

- **Columns Preserved**: All columns from the RBI Excel file are preserved as-is
- **Date Identifier**: First column with "date" in the name is used for upsert; if none found, first column is used
- **Duplicate Prevention**: Rows with the same date identifier are updated, not duplicated

## Customization

### Change Update Frequency

Edit `.github/workflows/fetch-data.yml`:
- **Every 30 minutes**: Change `'0 * * * *'` to `'*/30 * * * *'`
- **Every 2 hours**: Change to `'0 */2 * * *'`
- **Daily at specific time** (e.g., 9 AM UTC): Change to `'0 9 * * *'`

### Customize Logging

Edit `fetch_and_update.py` `logging.basicConfig()` section to:
- Change log level from `INFO` to `DEBUG` for more details
- Adjust log format as needed

### Handle Different Excel URLs

Edit the `RBI_EXCEL_URL` constant in the script and redeploy.

## Security Notes

⚠️ **Never commit credentials to Git**:
- `.gitignore` already excludes `service_account.json` and `.env`
- Always use GitHub Secrets for credentials
- Rotate Service Account keys periodically

## Support & Troubleshooting

1. Check `rbi_data_sync.log` in GitHub Actions artifacts
2. Review GitHub Actions workflow logs
3. Verify all environment variables are set correctly in GitHub Secrets
4. Ensure Google Sheet is shared with Service Account email

## License

MIT License - Feel free to modify and distribute

---

**Last Updated**: April 2026  
**Python Version**: 3.11+  
**Dependencies**: See `requirements.txt`
