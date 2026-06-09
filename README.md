# BeProduct Data Browser

Local desktop application for browsing and exploring BeProduct data with offline access and advanced search capabilities.

## 🎯 Overview

BeProduct Data Browser is a Streamlit-based desktop application that provides:

- **Offline Data Access** - Local SQLite database for fast queries
- **Advanced Search** - Full-text search across styles, materials, colors
- **Sync Management** - Pull data from BeProduct API
- **Data Export** - Export to Excel, CSV, JSON
- **Image Preview** - View style images inline
- **Change Tracking** - Track modifications and sync status

## 📸 Screenshots

[Add screenshots here]

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- BeProduct API credentials (OAuth)

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/beproduct-data-browser.git
cd beproduct-data-browser

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt
```

### Configuration

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Fill in your BeProduct credentials in `.env`:
   ```
   BEPRODUCT_CLIENT_ID=your_client_id
   BEPRODUCT_CLIENT_SECRET=your_client_secret
   BEPRODUCT_REFRESH_TOKEN=your_refresh_token
   BEPRODUCT_COMPANY_DOMAIN=your_company_domain
   ```

### Run the Application

```bash
streamlit run app/main.py
```

The application will open in your browser at `http://localhost:8501`

## 📁 Project Structure

```
beproduct-data-browser/
├── app/
│   ├── main.py              # Main Streamlit app
│   ├── config.py            # Configuration management
│   ├── pages/
│   │   ├── 1_Styles.py     # Styles browser
│   │   ├── 2_Materials.py  # Materials browser
│   │   ├── 3_Colors.py     # Colors browser
│   │   └── 4_Settings.py   # App settings
│   └── utils/
│       ├── api.py           # BeProduct API client
│       ├── database.py      # SQLite database manager
│       └── sync.py          # Data sync utilities
│
├── data/
│   └── beproduct.db        # Local SQLite database
│
├── tests/
│   └── test_api.py
│
├── .env.example            # Environment template
├── .env                    # Your credentials (gitignored)
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## 🔄 Data Sync

### Full Sync

Pulls all data from BeProduct and rebuilds the local database:

```bash
# In the app Settings page, click "Full Sync"
# Or run manually:
python -m app.utils.sync --full
```

### Incremental Sync

Pulls only changes since last sync:

```bash
# In the app Settings page, click "Sync"
# Or run manually:
python -m app.utils.sync --incremental
```

### Auto Sync

Configure automatic sync interval in Settings (default: 15 minutes)

## 🔍 Features

### Styles Browser
- Search by style number, description, team, season, year
- Filter by product status, category, brand
- View style images
- Export search results
- View full style details

### Materials Browser
- Search materials by name, supplier, content
- Filter by material type, category
- View material specifications
- Export material lists

### Colors Browser
- Search colors by name, code, season
- Visual color swatches
- Filter by active/inactive status
- Export color palettes

### Data Export
- **Excel** - Formatted spreadsheets with headers
- **CSV** - For data analysis tools
- **JSON** - For API/integration use

## 🛠️ Development

### Running Tests

```bash
pytest tests/
```

### Code Style

```bash
# Format code
black app/ tests/

# Lint
pylint app/ tests/
```

## 📊 Related Projects

For enterprise-scale data synchronization to Databricks:

👉 **[beproduct-databricks-sync](https://github.com/your-org/beproduct-databricks-sync)**

Features:
- DTC worksheet sync with change tracking
- BeProduct STYLE bi-directional sync
- Master data sync
- Delta Lake storage
- Scheduled job workflows

## 🔐 Security

- All credentials stored in `.env` (never committed to git)
- OAuth 2.0 for BeProduct API
- Local SQLite database (no external data sharing)
- Refresh tokens for secure API access

## 📝 License

[Your License Here]

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📞 Support

- **Issues:** GitHub Issues
- **Documentation:** [Wiki](wiki)
- **Contact:** [Your Team Email]

---

**Version:** 2.0.0  
**Last Updated:** 2026-06-09  
**Status:** Active Development
