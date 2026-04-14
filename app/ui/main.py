"""
BeProduct Data Browser — Streamlit entrypoint.

Run with:
    streamlit run app/ui/main.py

The app:
 1. Initialises the SQLite schema on first start
 2. Starts the APScheduler background sync job
 3. Renders sidebar navigation + main content area
"""

from __future__ import annotations

import logging
import sys
import os
import atexit

import streamlit as st

# Ensure project root is on sys.path so `app.*` imports work when run via
# `streamlit run app/ui/main.py` from the project root directory.
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

# ── Streamlit page config ──────────────────────────────────────────────────
st.set_page_config(
    page_title="BeProduct Data Browser",
    page_icon="🧶",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── One-time startup (per process) ────────────────────────────────────────
@st.cache_resource
def _startup():
    """Run once per Streamlit process. Initialise DB and background scheduler."""
    from app import db
    from app.config import settings

    # Initialise database schema
    db.init_schema()
    logging.getLogger(__name__).info("Database schema initialised")

    # Start APScheduler background sync
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from app.sync import scheduled_incremental_sync

        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(
            scheduled_incremental_sync,
            trigger="interval",
            minutes=settings.SYNC_INTERVAL_MINUTES,
            id="incremental_sync",
            replace_existing=True,
        )
        scheduler.start()
        logging.getLogger(__name__).info(
            f"Background sync scheduler started (every {settings.SYNC_INTERVAL_MINUTES} min)"
        )
        
        # Register shutdown handler to stop scheduler cleanly
        def _shutdown():
            logging.getLogger(__name__).info("Shutting down background scheduler...")
            scheduler.shutdown(wait=False)
            logging.getLogger(__name__).info("Scheduler shut down complete")
        
        atexit.register(_shutdown)
        
        return scheduler
    except Exception as e:
        logging.getLogger(__name__).warning(f"Could not start scheduler: {e}")
        return None


# ── Main ──────────────────────────────────────────────────────────────────
def main() -> None:
    # Validate config and boot
    try:
        _startup()
    except EnvironmentError as e:
        st.error(f"⚠️ Configuration error: {e}")
        st.info(
            "Please copy `.env.example` to `.env` and fill in your BeProduct credentials. "
            "Then restart the app."
        )
        st.stop()
    except Exception as e:
        st.error(f"⚠️ Startup error: {e}")
        st.stop()

    # Render sidebar and get page selection
    from app.ui.sidebar import render_sidebar
    page = render_sidebar()

    # Route to the selected page
    if "Styles" in page:
        from app.ui.styles_page import render_styles_page
        render_styles_page()

    elif "Materials" in page:
        from app.ui.materials_page import render_materials_page
        render_materials_page()

    elif "Colors" in page:
        from app.ui.colors_page import render_colors_page
        render_colors_page()

    elif "Images" in page:
        from app.ui.images_page import render_images_page
        render_images_page()

    elif "Blocks" in page:
        from app.ui.blocks_page import render_blocks_page
        render_blocks_page()

    elif "Directory" in page:
        from app.ui.directory_page import render_directory_page
        render_directory_page()

    elif "Users" in page:
        from app.ui.users_page import render_users_page
        render_users_page()

    else:  # Overview
        from app.ui.overview_page import render_overview_page
        render_overview_page()


if __name__ == "__main__":
    main()
