#!/usr/bin/env python3
"""
Hygenco News & Tenders Monitor - Entry Point
Run this to start the application.
"""

import uvicorn
from pathlib import Path

# Ensure data directory exists
data_dir = Path(__file__).parent / "data"
data_dir.mkdir(exist_ok=True)

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║  🌿 HYGENCO News & Tenders Monitor                        ║
    ║  Production-grade scraping agent with monitoring          ║
    ╠═══════════════════════════════════════════════════════════╣
    ║  Dashboard: http://localhost:8000                         ║
    ║  API Docs:  http://localhost:8000/docs                    ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
