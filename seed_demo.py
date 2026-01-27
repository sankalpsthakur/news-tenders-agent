#!/usr/bin/env python3
"""
Seed realistic demo data for Hygenco News & Tenders Monitor.
Creates sample runs and news items for the past 7 days with production-like data.
"""

import random
import hashlib
from datetime import datetime, timedelta, timezone
from app.database import init_db, get_db, Run, NewsItem, Source, Setting, Notification

# Indian states for project locations
STATES = ["Rajasthan", "Gujarat", "Tamil Nadu", "Karnataka", "Andhra Pradesh",
          "Maharashtra", "Madhya Pradesh", "Telangana", "Kerala", "Odisha"]

# Realistic MNRE tender data with REAL URLs
MNRE_TENDERS = [
    {"title": "Selection of Solar Power Developers for Setting up of 1500 MW ISTS-Connected Solar PV Power Projects", "type": "RfS", "url": "https://mnre.gov.in/en/tender/", "item_type": "tender"},
    {"title": "EoI for Development of Green Hydrogen Hubs across India under National Green Hydrogen Mission", "type": "EoI", "url": "https://nghm.mnre.gov.in/", "item_type": "tender"},
    {"title": "Tender for Supply and Installation of 10,000 Solar Water Pumping Systems under PM-KUSUM", "type": "Tender", "url": "https://mnre.gov.in/en/pradhan-mantri-kisan-urja-suraksha-evam-utthaan-mahabhiyaan-pm-kusum/", "item_type": "tender"},
    {"title": "Request for Proposal for Implementation of Grid-Connected Rooftop Solar Programme Phase-II", "type": "RfP", "url": "https://mnre.gov.in/en/notice-category/current-notices/", "item_type": "tender"},
    {"title": "Notice Inviting Tender for O&M of 50 MW Solar Power Plant at Bhadla Solar Park", "type": "NIT", "url": "https://mnre.gov.in/en/past-notices/tenders/", "item_type": "tender"},
    {"title": "EoI for Setting up of Offshore Wind Energy Projects in Tamil Nadu and Gujarat Coast", "type": "EoI", "url": "https://niwe.res.in/tenders.php", "item_type": "tender"},
    {"title": "Tender for Procurement of Battery Energy Storage Systems (BESS) - 500 MWh Capacity", "type": "Tender", "url": "https://mnre.gov.in/en/energy-storage-systemsess-projects-and-tenders/", "item_type": "tender"},
    {"title": "RfP for Development of Ultra Mega Renewable Energy Power Parks (UMREPP)", "type": "RfP", "url": "https://mnre.gov.in/en/documents/", "item_type": "tender"},
    {"title": "Selection of Bidders for 800 MW Wind Power Projects under Tranche-XIV", "type": "RfS", "url": "https://niwe.res.in/tenders.php", "item_type": "tender"},
    {"title": "Tender for Manufacturing of High-Efficiency Solar Cells and Modules under PLI Scheme", "type": "Tender", "url": "https://nise.res.in/?s=tenders", "item_type": "tender"},
    {"title": "Notice for Empanelment of Agencies for Solar Rooftop Installation", "type": "Notice", "url": "https://mnre.gov.in/en/notice-category/current-notices/", "item_type": "tender"},
    {"title": "EoI for Pilot Projects on Green Ammonia Production using Renewable Energy", "type": "EoI", "url": "https://nghm.mnre.gov.in/", "item_type": "tender"},
    {"title": "Tender for Smart Grid Infrastructure Development in 5 Smart Cities", "type": "Tender", "url": "https://www.ireda.in/tender", "item_type": "tender"},
    {"title": "RfP for Consultancy Services for National Renewable Energy Assessment", "type": "RfP", "url": "https://mnre.gov.in/en/annual-report-2024-25/", "item_type": "tender"},
    {"title": "Selection of Project Developers for 300 MW Floating Solar Projects", "type": "RfS", "url": "https://mnre.gov.in/en/tender/", "item_type": "tender"},
]

# Realistic SECI news/announcements with REAL URLs
SECI_NEWS = [
    {"title": "SECI Announces Results of e-Reverse Auction for 1200 MW ISTS Solar Projects", "type": "Results", "url": "https://www.seci.co.in/tenders", "item_type": "news"},
    {"title": "Amendment-III to RfS for 1500 MW Tranche-XII Wind Power Projects", "type": "Amendment", "url": "https://www.seci.co.in/tenders/archive", "item_type": "news"},
    {"title": "SECI Issues Letter of Award (LoA) to Successful Bidders for 2000 MW Hybrid Projects", "type": "LoA", "url": "https://www.seci.co.in/latest-news", "item_type": "news"},
    {"title": "Pre-Bid Meeting Minutes for 500 MW Peak Power Supply from BESS", "type": "Minutes", "url": "https://www.seci.co.in/uploads/tenders/corrigendums/Pre-bid_meeting_notification29.pdf", "item_type": "news"},
    {"title": "Corrigendum to RfS No. SECI/C&P/2025/001 for Manufacturing Linked Solar Tender", "type": "Corrigendum", "url": "https://www.seci.co.in/tenders", "item_type": "news"},
    {"title": "SECI Signs Power Sale Agreement with NHPC for 1000 MW RE Power", "type": "PSA", "url": "https://www.seci.co.in/press-release", "item_type": "news"},
    {"title": "Notification: Revised Guidelines for Wind-Solar Hybrid Projects with Storage", "type": "Notification", "url": "https://www.seci.co.in/latest-news", "item_type": "news"},
    {"title": "SECI Invites Bids for 400 MW Round-the-Clock (RTC) Power from RE Sources", "type": "RfS", "url": "https://www.seci.co.in/tenders", "item_type": "tender"},
    {"title": "Results Declared: Tariff of Rs 2.54/kWh Discovered for Gujarat Solar Park", "type": "Results", "url": "https://www.seci.co.in/press-release", "item_type": "news"},
    {"title": "SECI Issues Clarifications to Pre-Bid Queries for Green Hydrogen Tender", "type": "Clarification", "url": "https://www.seci.co.in/tenders", "item_type": "news"},
    {"title": "Extension of Bid Submission Date for 750 MW FDRE Tender", "type": "Extension", "url": "https://www.seci.co.in/tenders/archive", "item_type": "news"},
    {"title": "SECI Announces Successful Grid Synchronization of 500 MW Wind Project", "type": "Announcement", "url": "https://www.seci.co.in/press-release", "item_type": "news"},
    {"title": "Request for Selection of Solar Developers for 600 MW Projects with ALMM Compliance", "type": "RfS", "url": "https://www.seci.co.in/tenders", "item_type": "tender"},
    {"title": "SECI Releases Draft RfS for Procurement of 1000 MW Firm Power from RE with Storage", "type": "Draft", "url": "https://www.seci.co.in/latest-news", "item_type": "news"},
    {"title": "Notice: Revised Timelines for Commissioning of Projects under Tranche-X", "type": "Notice", "url": "https://www.seci.co.in/tenders", "item_type": "tender"},
]

def generate_content_hash(source: str, title: str, url: str) -> str:
    """Generate unique SHA256 hash for deduplication."""
    content = f"{source}:{title}:{url}:{datetime.now(timezone.utc).isoformat()}"
    return hashlib.sha256(content.encode()).hexdigest()

# URL generation functions removed - now using real URLs from data

def seed_database():
    """Seed the database with realistic demo data."""
    print("🌱 Initializing database...")
    init_db()

    with get_db() as db:
        # Clear existing demo data
        print("🧹 Clearing existing data...")
        db.query(Notification).delete()
        db.query(NewsItem).delete()
        db.query(Run).delete()
        db.commit()

        print("📊 Creating realistic demo data for past 7 days...\n")

        now = datetime.now(timezone.utc)
        total_news_items = 0
        total_runs = 0
        total_notifications = 0

        # Track used items to avoid exact duplicates in display
        used_mnre = []
        used_seci = []

        # Create runs for each day (past 7 days + today)
        for days_ago in range(7, -1, -1):
            run_date = now - timedelta(days=days_ago)
            day_str = run_date.strftime("%a %b %d")

            # Scheduled morning run at 6:00 AM IST (00:30 UTC)
            morning_run = run_date.replace(hour=0, minute=30, second=0, microsecond=0)

            # Determine run status (realistic distribution)
            rand = random.random()
            if rand < 0.80:
                status = "success"
            elif rand < 0.95:
                status = "partial"
            else:
                status = "failed"

            # Realistic duration (20-60 seconds for scraping)
            duration = round(random.uniform(22, 58), 2)

            # Items found decreases slightly over time (as more are already in DB)
            base_items = random.randint(4, 8) if days_ago > 3 else random.randint(2, 5)
            items_found = base_items if status != "failed" else 0

            # New items also decrease (first runs find more new items)
            if status == "failed":
                new_items = 0
            elif days_ago >= 5:
                new_items = random.randint(2, min(items_found, 5))
            elif days_ago >= 2:
                new_items = random.randint(1, min(items_found, 3))
            else:
                new_items = random.randint(0, min(items_found, 2))

            # Create the run record
            run = Run(
                started_at=morning_run,
                completed_at=morning_run + timedelta(seconds=duration),
                status=status,
                sources_scraped='["mnre", "seci"]' if status != "failed" else '["mnre"]' if status == "partial" else '[]',
                items_found=items_found,
                new_items=new_items,
                triggered_by="schedule",
                duration_seconds=duration,
                error_message="Connection timeout while fetching SECI website. Retried 3 times." if status == "partial" else
                             "Failed to connect to MNRE server. Network unreachable." if status == "failed" else None
            )
            db.add(run)
            db.commit()
            db.refresh(run)
            total_runs += 1

            print(f"  {day_str}: Run #{run.id} [{status.upper()}] - {items_found} items, {new_items} new")

            # Create news items for this run
            if status in ["success", "partial"]:
                # MNRE items (40-60% of items) - TENDERS
                mnre_count = random.randint(1, max(1, items_found // 2))
                for i in range(mnre_count):
                    tender = random.choice([t for t in MNRE_TENDERS if t["title"] not in used_mnre] or MNRE_TENDERS)
                    used_mnre.append(tender["title"])
                    if len(used_mnre) > 10:
                        used_mnre.pop(0)

                    pub_date = (morning_run - timedelta(days=random.randint(0, 3))).strftime("%d-%b-%Y")
                    url = tender["url"]  # Use real URL from data
                    title = tender["title"]

                    item = NewsItem(
                        source="mnre",
                        title=f"[{tender['type']}] {title}",
                        url=url,
                        published_date=pub_date,
                        content_hash=generate_content_hash("mnre", title, url),
                        run_id=run.id,
                        created_at=morning_run + timedelta(seconds=random.randint(5, int(duration) - 5)),
                        is_new=(i < new_items // 2 + 1),
                        item_type=tender["item_type"]
                    )
                    db.add(item)
                    total_news_items += 1

                # SECI items (remaining items) - NEWS/TENDERS
                seci_count = items_found - mnre_count
                for i in range(seci_count):
                    news = random.choice([n for n in SECI_NEWS if n["title"] not in used_seci] or SECI_NEWS)
                    used_seci.append(news["title"])
                    if len(used_seci) > 10:
                        used_seci.pop(0)

                    url = news["url"]  # Use real URL from data
                    title = news["title"]

                    item = NewsItem(
                        source="seci",
                        title=f"[{news['type']}] {title}",
                        url=url,
                        published_date=morning_run.strftime("%d-%b-%Y"),
                        content_hash=generate_content_hash("seci", title, url),
                        run_id=run.id,
                        created_at=morning_run + timedelta(seconds=random.randint(5, int(duration) - 5)),
                        is_new=(i < (new_items - new_items // 2)),
                        item_type=news["item_type"]
                    )
                    db.add(item)
                    total_news_items += 1

            # Create Teams notification for runs with new items
            if new_items > 0 and status == "success":
                notification = Notification(
                    run_id=run.id,
                    channel="teams",
                    message=f"🌿 Hygenco Alert: Found {new_items} new tender(s)/announcement(s) from MNRE and SECI. Check dashboard for details.",
                    status="sent",
                    sent_at=morning_run + timedelta(seconds=duration + 3)
                )
                db.add(notification)
                total_notifications += 1

            db.commit()

            # Optional: Manual afternoon run on some days
            if days_ago in [2, 5] and random.random() < 0.7:
                afternoon_run = run_date.replace(hour=9, minute=0, second=0, microsecond=0)  # 2:30 PM IST
                afternoon_duration = round(random.uniform(18, 35), 2)
                afternoon_items = random.randint(1, 3)
                afternoon_new = random.randint(0, 1)

                manual_run = Run(
                    started_at=afternoon_run,
                    completed_at=afternoon_run + timedelta(seconds=afternoon_duration),
                    status="success",
                    sources_scraped='["mnre", "seci"]',
                    items_found=afternoon_items,
                    new_items=afternoon_new,
                    triggered_by="manual",
                    duration_seconds=afternoon_duration
                )
                db.add(manual_run)
                db.commit()
                total_runs += 1
                print(f"  {day_str}: Run #{manual_run.id} [MANUAL] - {afternoon_items} items, {afternoon_new} new")

        # Update settings with Teams webhook
        print("\n⚙️  Configuring settings...")
        settings_updates = [
            ("teams_webhook_url", "https://hygencoin.webhook.office.com/webhookb2/f2ec3478-9d11-4553-8c65-6708e56da9e6@7ae7c16b-7491-45c1-b6a7-c4dc469742af/IncomingWebhook/b7deab2cc69047eeb362ec0dba559631/07aa046b-a66f-434c-9239-3a22dfe3e09c"),
            ("notification_enabled", "true"),
            ("schedule_enabled", "true"),
            ("schedule_time", "06:00"),
        ]

        for key, value in settings_updates:
            setting = db.query(Setting).filter(Setting.key == key).first()
            if setting:
                setting.value = value
            else:
                db.add(Setting(key=key, value=value))
        db.commit()

        print(f"""
╔══════════════════════════════════════════════════════════════╗
║  ✅ Demo Data Seeded Successfully!                           ║
╠══════════════════════════════════════════════════════════════╣
║  📊 Runs created:          {total_runs:>3}                               ║
║  📰 News items created:    {total_news_items:>3}                               ║
║  📨 Notifications sent:    {total_notifications:>3}                               ║
║  🔗 Teams webhook:         Configured                        ║
╠══════════════════════════════════════════════════════════════╣
║  🌐 Dashboard: http://localhost:8000                         ║
║  📚 API Docs:  http://localhost:8000/docs                    ║
╚══════════════════════════════════════════════════════════════╝
""")

if __name__ == "__main__":
    seed_database()
