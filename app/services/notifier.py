"""
Notification service for Hygenco News & Tenders Monitor.

Handles sending notifications to Microsoft Teams, Email (SMTP), and other channels,
and maintains notification history in the database.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import httpx
import aiosmtplib

from app.database import get_db, Run, Notification, Setting, NewsItem, Connector
from app.models import NotificationStatus, NotificationChannel, NotificationResponse
from app.config import settings

logger = logging.getLogger(__name__)


class NotifierService:
    """
    Manages notifications for scraping run results.

    Supports Microsoft Teams webhooks and maintains a history of all
    notifications sent.
    """

    def __init__(self):
        """Initialize the notifier service."""
        self._http_client: Optional[httpx.Client] = None

    @property
    def http_client(self) -> httpx.Client:
        """Lazy-initialize HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=30.0)
        return self._http_client

    def _get_teams_webhook_url(self) -> Optional[str]:
        """
        Get Teams webhook URL from database settings or config.

        Returns:
            Webhook URL if configured, None otherwise
        """
        # First check database settings
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "teams_webhook_url").first()
            if setting and setting.value:
                return setting.value

        # Fall back to config
        return settings.teams_webhook_url

    def _is_notification_enabled(self) -> bool:
        """
        Check if notifications are enabled in settings.

        Returns:
            True if notifications are enabled
        """
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "notification_enabled").first()
            if setting:
                return setting.value.lower() == "true"
        return True

    def _format_teams_message(
        self,
        run_id: int,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Format a message payload for Microsoft Teams webhook.

        Args:
            run_id: The run ID to report on
            message: Optional custom message

        Returns:
            Teams Adaptive Card message payload
        """
        with get_db() as db:
            run = db.query(Run).filter(Run.id == run_id).first()
            if not run:
                return self._create_simple_teams_message(
                    message or f"Run {run_id} not found"
                )

            # Get source breakdown
            sources_scraped = run.get_sources_scraped()
            source_breakdown = []

            for source_code in sources_scraped:
                count = db.query(NewsItem).filter(
                    NewsItem.run_id == run_id,
                    NewsItem.source == source_code
                ).count()
                source_breakdown.append(f"- **{source_code.upper()}**: {count} new items")

            # Determine status emoji and color
            status_info = {
                "success": {"emoji": "✅", "color": "good"},
                "partial": {"emoji": "⚠️", "color": "warning"},
                "failed": {"emoji": "❌", "color": "attention"},
                "running": {"emoji": "🔄", "color": "default"}
            }

            info = status_info.get(run.status, {"emoji": "❓", "color": "default"})

            # Format duration
            duration_str = ""
            if run.duration_seconds:
                if run.duration_seconds < 60:
                    duration_str = f"{run.duration_seconds:.1f} seconds"
                else:
                    minutes = int(run.duration_seconds // 60)
                    seconds = run.duration_seconds % 60
                    duration_str = f"{minutes}m {seconds:.0f}s"

            # Build Adaptive Card payload
            card = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "0076D7" if run.status == "success" else "FF0000",
                "summary": f"Hygenco News Monitor - Run #{run_id}",
                "sections": [
                    {
                        "activityTitle": f"{info['emoji']} Hygenco News & Tenders Monitor",
                        "activitySubtitle": f"Run #{run_id} - {run.status.upper()}",
                        "facts": [
                            {"name": "Status", "value": run.status.upper()},
                            {"name": "Triggered By", "value": run.triggered_by},
                            {"name": "Items Found", "value": str(run.items_found or 0)},
                            {"name": "New Items", "value": str(run.new_items or 0)},
                            {"name": "Duration", "value": duration_str or "N/A"},
                            {"name": "Completed At", "value": run.completed_at.strftime("%Y-%m-%d %H:%M:%S UTC") if run.completed_at else "In Progress"}
                        ],
                        "markdown": True
                    }
                ]
            }

            # Add source breakdown if there are new items
            if source_breakdown and run.new_items > 0:
                card["sections"].append({
                    "title": "Source Breakdown",
                    "text": "\n".join(source_breakdown),
                    "markdown": True
                })

            # Add custom message if provided
            if message:
                card["sections"].append({
                    "text": message,
                    "markdown": True
                })

            # Add error message if present
            if run.error_message:
                card["sections"].append({
                    "title": "Errors",
                    "text": f"```\n{run.error_message}\n```",
                    "markdown": True
                })

            return card

    def _create_simple_teams_message(self, text: str) -> Dict[str, Any]:
        """
        Create a simple text message for Teams.

        Args:
            text: Message text

        Returns:
            Simple Teams message payload
        """
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": "Hygenco News Monitor",
            "themeColor": "0076D7",
            "text": text
        }

    def send_teams_notification(
        self,
        run_id: int,
        message: Optional[str] = None
    ) -> NotificationResponse:
        """
        Send a notification to Microsoft Teams about a run.

        Args:
            run_id: The run ID to report on
            message: Optional custom message to include

        Returns:
            NotificationResponse with status and details
        """
        notification_record: Optional[Notification] = None

        with get_db() as db:
            # Create notification record
            notification_record = Notification(
                run_id=run_id,
                channel=NotificationChannel.TEAMS.value,
                status=NotificationStatus.PENDING.value
            )
            db.add(notification_record)
            db.commit()
            db.refresh(notification_record)
            notification_id = notification_record.id

        # Check if notifications are enabled
        if not self._is_notification_enabled():
            return self._update_notification_status(
                notification_id,
                NotificationStatus.FAILED,
                error_message="Notifications are disabled"
            )

        # Get webhook URL
        webhook_url = self._get_teams_webhook_url()
        if not webhook_url:
            return self._update_notification_status(
                notification_id,
                NotificationStatus.FAILED,
                error_message="Teams webhook URL not configured"
            )

        # Format message
        payload = self._format_teams_message(run_id, message)

        try:
            logger.info(f"Sending Teams notification for run {run_id}")

            response = self.http_client.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()

            logger.info(f"Teams notification sent successfully for run {run_id}")

            return self._update_notification_status(
                notification_id,
                NotificationStatus.SENT,
                message=str(payload)
            )

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            logger.error(f"Failed to send Teams notification: {error_msg}")
            return self._update_notification_status(
                notification_id,
                NotificationStatus.FAILED,
                error_message=error_msg
            )

        except httpx.RequestError as e:
            error_msg = f"Request error: {str(e)}"
            logger.error(f"Failed to send Teams notification: {error_msg}")
            return self._update_notification_status(
                notification_id,
                NotificationStatus.FAILED,
                error_message=error_msg
            )

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"Failed to send Teams notification: {error_msg}", exc_info=True)
            return self._update_notification_status(
                notification_id,
                NotificationStatus.FAILED,
                error_message=error_msg
            )

    def _update_notification_status(
        self,
        notification_id: int,
        status: NotificationStatus,
        message: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> NotificationResponse:
        """
        Update notification record status.

        Args:
            notification_id: ID of the notification record
            status: New status
            message: Optional message content
            error_message: Optional error message

        Returns:
            Updated NotificationResponse
        """
        with get_db() as db:
            notification = db.query(Notification).filter(
                Notification.id == notification_id
            ).first()

            if notification:
                notification.status = status.value
                if message:
                    notification.message = message
                if error_message:
                    notification.error_message = error_message
                if status == NotificationStatus.SENT:
                    notification.sent_at = datetime.utcnow()

                db.commit()
                db.refresh(notification)

                return NotificationResponse(
                    id=notification.id,
                    run_id=notification.run_id,
                    channel=notification.channel,
                    message=notification.message,
                    status=notification.status,
                    sent_at=notification.sent_at,
                    error_message=notification.error_message
                )

            # Return a default response if notification not found
            return NotificationResponse(
                id=notification_id,
                run_id=None,
                channel=NotificationChannel.TEAMS.value,
                status=NotificationStatus.FAILED.value,
                error_message="Notification record not found"
            )

    def get_notifications_for_run(self, run_id: int) -> List[NotificationResponse]:
        """
        Get all notifications for a specific run.

        Args:
            run_id: The run ID

        Returns:
            List of NotificationResponse objects
        """
        with get_db() as db:
            notifications = db.query(Notification).filter(
                Notification.run_id == run_id
            ).all()

            return [
                NotificationResponse(
                    id=n.id,
                    run_id=n.run_id,
                    channel=n.channel,
                    message=n.message,
                    status=n.status,
                    sent_at=n.sent_at,
                    error_message=n.error_message
                )
                for n in notifications
            ]

    def close(self):
        """Close the HTTP client."""
        if self._http_client:
            self._http_client.close()
            self._http_client = None

    def _get_email_connector_config(self) -> Optional[Dict[str, Any]]:
        """
        Get email connector configuration from database.

        Returns:
            Email connector config dict if configured and enabled, None otherwise
        """
        with get_db() as db:
            connector = db.query(Connector).filter(
                Connector.channel_type == "email"
            ).first()

            if not connector:
                logger.warning("Email connector not found in database")
                return None

            if not connector.enabled:
                logger.warning("Email connector is disabled")
                return None

            config = connector.get_config()
            required_fields = ["smtp_host", "smtp_port", "from_address"]

            for field in required_fields:
                if not config.get(field):
                    logger.warning(f"Email connector missing required field: {field}")
                    return None

            return config


async def send_email(
    to_addresses: List[str],
    subject: str,
    body_html: str,
    attachments: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Send an email using SMTP with TLS support.

    Args:
        to_addresses: List of recipient email addresses
        subject: Email subject line
        body_html: HTML body content
        attachments: Optional list of attachments, each with:
            - filename: Name for the attachment
            - content: Bytes content of the attachment
            - content_type: MIME type (e.g., 'application/pdf')

    Returns:
        Dict with 'success' boolean and 'message' or 'error' key
    """
    # Get email connector config
    with get_db() as db:
        connector = db.query(Connector).filter(
            Connector.channel_type == "email"
        ).first()

        if not connector:
            return {
                "success": False,
                "error": "Email connector not configured"
            }

        if not connector.enabled:
            return {
                "success": False,
                "error": "Email connector is disabled"
            }

        config = connector.get_config()

    # Validate required config
    smtp_host = config.get("smtp_host")
    smtp_port = config.get("smtp_port", 587)
    smtp_username = config.get("smtp_user") or config.get("smtp_username")
    smtp_password = config.get("smtp_password")
    from_address = config.get("from_address")
    use_tls = config.get("use_tls", True)

    if not smtp_host or not from_address:
        return {
            "success": False,
            "error": "Email connector missing smtp_host or from_address"
        }

    if not to_addresses:
        return {
            "success": False,
            "error": "No recipient addresses provided"
        }

    try:
        # Create message
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = from_address
        msg["To"] = ", ".join(to_addresses)

        # Add HTML body
        html_part = MIMEText(body_html, "html", "utf-8")
        msg.attach(html_part)

        # Add attachments
        if attachments:
            for attachment in attachments:
                filename = attachment.get("filename", "attachment")
                content = attachment.get("content")
                content_type = attachment.get("content_type", "application/octet-stream")

                if content:
                    main_type, sub_type = content_type.split("/", 1)
                    part = MIMEBase(main_type, sub_type)
                    part.set_payload(content)
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f'attachment; filename="{filename}"'
                    )
                    msg.attach(part)

        # Connect and send
        logger.info(f"Connecting to SMTP server {smtp_host}:{smtp_port}")

        if use_tls:
            # Use STARTTLS
            smtp_client = aiosmtplib.SMTP(
                hostname=smtp_host,
                port=smtp_port,
                start_tls=True
            )
        else:
            smtp_client = aiosmtplib.SMTP(
                hostname=smtp_host,
                port=smtp_port
            )

        await smtp_client.connect()

        # Authenticate if credentials provided
        if smtp_username and smtp_password:
            await smtp_client.login(smtp_username, smtp_password)

        # Send email
        await smtp_client.send_message(msg)
        await smtp_client.quit()

        logger.info(f"Email sent successfully to {len(to_addresses)} recipients")
        return {
            "success": True,
            "message": f"Email sent to {len(to_addresses)} recipients"
        }

    except aiosmtplib.SMTPException as e:
        error_msg = f"SMTP error: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Failed to send email: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "success": False,
            "error": error_msg
        }


async def send_report_email(
    report_pdf_bytes: bytes,
    recipient_emails: List[str],
    report_period: str,
    report_start_date: Optional[str] = None,
    report_end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send a report email with PDF attachment.

    Args:
        report_pdf_bytes: PDF report content as bytes
        recipient_emails: List of recipient email addresses
        report_period: Period name (e.g., 'weekly', 'monthly', 'quarterly')
        report_start_date: Optional start date string for the report period
        report_end_date: Optional end date string for the report period

    Returns:
        Dict with 'success' boolean and 'message' or 'error' key
    """
    # Format dates for display
    date_range = ""
    if report_start_date and report_end_date:
        date_range = f" ({report_start_date} to {report_end_date})"

    # Generate filename
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    filename = f"hygenco_news_report_{report_period}_{timestamp}.pdf"

    # Create professional email template
    subject = f"Hygenco News & Tenders {report_period.capitalize()} Report{date_range}"

    body_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{
                font-family: Arial, Helvetica, sans-serif;
                line-height: 1.6;
                color: #333333;
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
            }}
            .header {{
                background: linear-gradient(135deg, #1a5f7a 0%, #2e8b57 100%);
                color: white;
                padding: 30px;
                text-align: center;
                border-radius: 8px 8px 0 0;
            }}
            .header h1 {{
                margin: 0;
                font-size: 24px;
            }}
            .header p {{
                margin: 10px 0 0 0;
                opacity: 0.9;
            }}
            .content {{
                background: #ffffff;
                padding: 30px;
                border: 1px solid #e0e0e0;
                border-top: none;
            }}
            .highlight {{
                background: #f8f9fa;
                padding: 15px;
                border-left: 4px solid #2e8b57;
                margin: 20px 0;
            }}
            .footer {{
                background: #f8f9fa;
                padding: 20px;
                text-align: center;
                font-size: 12px;
                color: #666666;
                border: 1px solid #e0e0e0;
                border-top: none;
                border-radius: 0 0 8px 8px;
            }}
            .button {{
                display: inline-block;
                background: #2e8b57;
                color: white;
                padding: 12px 24px;
                text-decoration: none;
                border-radius: 4px;
                margin-top: 15px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Hygenco News & Tenders Monitor</h1>
            <p>{report_period.capitalize()} Report</p>
        </div>
        <div class="content">
            <p>Dear Subscriber,</p>

            <p>Please find attached your <strong>{report_period}</strong> news and tenders report{date_range}.</p>

            <div class="highlight">
                <strong>Report Details:</strong><br>
                <ul style="margin: 10px 0 0 0; padding-left: 20px;">
                    <li>Period: {report_period.capitalize()}</li>
                    {"<li>Date Range: " + report_start_date + " to " + report_end_date + "</li>" if report_start_date and report_end_date else ""}
                    <li>Generated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}</li>
                </ul>
            </div>

            <p>This report contains:</p>
            <ul>
                <li>Summary statistics of news and tender items</li>
                <li>Source-wise breakdown of collected items</li>
                <li>Run execution history and success rates</li>
                <li>Daily breakdown of activity</li>
            </ul>

            <p>If you have any questions about this report, please contact your system administrator.</p>

            <p>Best regards,<br>
            <strong>Hygenco News Monitor System</strong></p>
        </div>
        <div class="footer">
            <p>This is an automated message from the Hygenco News & Tenders Monitor.</p>
            <p>To unsubscribe or manage your notification preferences, please contact your administrator.</p>
        </div>
    </body>
    </html>
    """

    # Prepare attachment
    attachments = [
        {
            "filename": filename,
            "content": report_pdf_bytes,
            "content_type": "application/pdf"
        }
    ]

    # Send email
    result = await send_email(
        to_addresses=recipient_emails,
        subject=subject,
        body_html=body_html,
        attachments=attachments
    )

    return result


# Singleton instance
notifier_service = NotifierService()
