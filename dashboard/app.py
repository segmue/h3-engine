"""
H3 Engine Dashboard - Main Application.

This dashboard provides:
- Tab 1: H3 Engine Sandbox - Interactive spatial queries on H3-indexed geodata
- Tab 2: Spatial Association - Matrix visualization of association results

Architecture:
- Shiny (Port 8000): UI, state management, engine calls
- FastAPI (Port 8001): MVT tile serving

Run with:
    shiny run dashboard/app.py --host 0.0.0.0 --port 8000 --reload
"""

import sys
from pathlib import Path

# Ensure dashboard package is importable
dashboard_dir = Path(__file__).parent
project_root = dashboard_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from shiny import App, Inputs, Outputs, Session, ui

from dashboard.config import log
from dashboard.pages.sandbox import sandbox_ui, sandbox_server
from dashboard.pages.association import association_ui, association_server
from dashboard.pages.sentence_gen import sentence_gen_ui, sentence_gen_server


# ============================================================================
# Main UI with Tabs
# ============================================================================

app_ui = ui.page_fluid(
    ui.navset_tab(
        ui.nav_panel("H3 Engine Sandbox", sandbox_ui),
        ui.nav_panel("Spatial Association", association_ui),
        ui.nav_panel("Sentence Generator", sentence_gen_ui),
        id="main_tabs"
    ),
)


# ============================================================================
# Server
# ============================================================================

def server(input: Inputs, output: Outputs, session: Session):
    """Main server combining all page servers."""
    log("Initializing dashboard...")

    # Initialize page servers
    sandbox_server(input, output, session)
    association_server(input, output, session)
    sentence_gen_server(input, output, session)

    log("Dashboard ready", "success")


app = App(app_ui, server)
