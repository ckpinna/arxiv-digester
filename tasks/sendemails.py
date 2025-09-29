import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Mapping
from prefect import task, get_run_logger
from prefect.blocks.system import Secret

smtp_pass = Secret.load("smtp-pass")
smtp_user = Secret.load("smtp-user")

def build_newsletter_html(filtered_papers: List[Mapping[str, str]]) -> str:
    """Builds an HTML newsletter from filtered arXiv papers."""
    items_html = []
    for p in filtered_papers:
        item = f"""
        <div style="margin-bottom:20px;">
          <h3><a href="{p.get('linkHtml','')}" target="_blank">{p.get('title','')}</a></h3>
          <p><strong>Authors:</strong> {p.get('authors','')}</p>
          <p><strong>Categories:</strong> {p.get('categories','')}</p>
          <p><strong>Published:</strong> {p.get('published','')}</p>
          <p>{p.get('summary','')}</p>
          <p>
            [<a href="{p.get('linkHtml','')}" target="_blank">Abstract</a>]
            [<a href="{p.get('linkPdf','')}" target="_blank">PDF</a>]
          </p>
        </div>
        """
        items_html.append(item)

    html = f"""
    <html>
      <body>
        <h2>arXiv Digest Newsletter</h2>
        {''.join(items_html)}
      </body>
    </html>
    """
    return html

def send_email(filtered_papers: List[Mapping[str, str]], recipients: List[str]):
    """
    Sends a newsletter email with the filtered papers to a list of recipients.
    """
    logger = get_run_logger()
    logger.info(f"Sending newsletter to {len(recipients)} recipients...")

    # --- Build email content
    subject = "The arXiv Digester"
    html_body = build_newsletter_html(filtered_papers)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = "you@example.com"       # change to your sender email
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))
    # --- Send via SMTP
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:  # change server if needed
            server.starttls()
            server.login(smtp_user.get(), smtp_pass.get())  # use app password / env vars
            server.sendmail(msg["From"], recipients, msg.as_string())
        logger.info("Newsletter sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
