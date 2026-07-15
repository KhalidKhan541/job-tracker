"""
Job Tracker Harness
Robust pipeline for scanning 52 companies via EXA search with email alerts.
Includes: retry logic, rate limiting, atomic state, pre-flight validation,
structured logging, failure isolation, and health alerts.
"""

import json
import os
import sys
import hashlib
import smtplib
import time
import tempfile
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
from functools import wraps

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class Logger:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.stats = {"companies_scanned": 0, "new_jobs": 0, "api_errors": 0, "companies_failed": []}

    def info(self, msg):
        print(f"[INFO] {msg}")

    def warn(self, msg):
        print(f"[WARN] {msg}")
        self.warnings.append(msg)

    def error(self, msg):
        print(f"[ERROR] {msg}")
        self.errors.append(msg)

    def company_ok(self, name, count):
        self.stats["companies_scanned"] += 1
        self.stats["new_jobs"] += count

    def company_fail(self, name, reason):
        self.stats["companies_scanned"] += 1
        self.stats["companies_failed"].append({"company": name, "reason": str(reason)})

    def api_error(self):
        self.stats["api_errors"] += 1

    def summary(self):
        return {
            **self.stats,
            "total_errors": len(self.errors),
            "total_warnings": len(self.warnings),
            "errors": self.errors,
            "warnings": self.warnings,
        }

log = Logger()

# ---------------------------------------------------------------------------
# Config Validation
# ---------------------------------------------------------------------------

REQUIRED_ENV = ["EXA_API_KEY", "SENDER_EMAIL", "SENDER_PASSWORD"]
OPTIONAL_ENV = {"RECEIVER_EMAIL": None}

def validate_config():
    """Pre-flight check: ensure all required secrets are set and non-empty."""
    missing = []
    for key in REQUIRED_ENV:
        val = os.environ.get(key, "").strip()
        if not val:
            missing.append(key)

    if missing:
        log.error(f"Missing required secrets: {', '.join(missing)}")
        log.error("Set them in GitHub repo → Settings → Secrets → Actions")
        sys.exit(1)

    # RECEIVER_EMAIL defaults to SENDER_EMAIL
    receiver = os.environ.get("RECEIVER_EMAIL", "").strip()
    if not receiver:
        os.environ["RECEIVER_EMAIL"] = os.environ["SENDER_EMAIL"]

    log.info("Config validation passed")

# ---------------------------------------------------------------------------
# Retry with exponential backoff
# ---------------------------------------------------------------------------

def retry(max_attempts=3, base_delay=2, backoff=2, retries_on=(Exception,)):
    """Decorator: retry on failure with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retries_on as e:
                    last_exc = e
                    if attempt < max_attempts:
                        delay = base_delay * (backoff ** (attempt - 1))
                        log.warn(f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}")
                        log.info(f"Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        log.error(f"All {max_attempts} attempts failed for {func.__name__}: {e}")
            raise last_exc
        return wrapper
    return decorator

# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Simple token-bucket rate limiter."""
    def __init__(self, min_interval=1.5):
        self.min_interval = min_interval
        self.last_call = 0

    def wait(self):
        now = time.time()
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            time.sleep(sleep_time)
        self.last_call = time.time()

rate_limiter = RateLimiter(min_interval=1.5)

# ---------------------------------------------------------------------------
# Atomic State Management
# ---------------------------------------------------------------------------

STATE_FILE = Path("seen_jobs.json")
COMPANIES_FILE = Path("companies.json")

def load_companies():
    with open(COMPANIES_FILE, "r") as f:
        data = json.load(f)
    return data["companies"]

def load_seen():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            log.warn("Corrupt seen_jobs.json - starting fresh")
            return {}
    return {}

def save_seen(seen):
    """Atomic write: write to temp file, then rename."""
    try:
        fd, tmp_path = tempfile.mkstemp(dir=STATE_FILE.parent, suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(seen, f, indent=2)
        Path(tmp_path).replace(STATE_FILE)
        log.info(f"State saved ({len(seen)} entries)")
    except Exception as e:
        log.error(f"Failed to save state: {e}")

def job_id(url):
    return hashlib.md5(url.encode()).hexdigest()

# ---------------------------------------------------------------------------
# EXA Search with Retry
# ---------------------------------------------------------------------------

SEARCH_TEMPLATES = [
    "{company} hiring Pakistan software engineer jobs",
    "{company} remote software engineer jobs 2025",
]

@retry(max_attempts=3, base_delay=3, backoff=2)
def search_exa(exa_client, query):
    """Single EXA search call with retry."""
    rate_limiter.wait()
    return exa_client.search(
        query,
        type="auto",
        num_results=10,
        contents={"highlights": True},
    )

def search_jobs(exa_client, company_name):
    """Search EXA for job postings at a company. Isolates per-query errors."""
    results = []
    for template in SEARCH_TEMPLATES:
        query = template.format(company=company_name)
        try:
            response = search_exa(exa_client, query)
            for r in response.results:
                results.append({
                    "title": r.title or "",
                    "url": r.url,
                    "published_date": str(r.published_date) if r.published_date else "",
                    "highlights": r.highlights if r.highlights else [],
                })
        except Exception as e:
            log.warn(f"Search failed for '{company_name}' query '{template}': {e}")
            log.api_error()
    return results

# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

JOB_KEYWORDS = [
    "job", "career", "hiring", "position", "apply", "opening",
    "vacancy", "recruit", "engineer", "developer", "remote",
    "intern", "graduate", "lead", "senior", "staff", "principal",
]

def filter_relevant(results, company_name):
    """Dedup by URL and filter out obvious non-jobs."""
    seen_urls = set()
    filtered = []
    company_slug = company_name.lower().replace(" ", "")

    for r in results:
        url = r["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)

        title_lower = r["title"].lower()
        highlights_text = " ".join(r["highlights"]).lower() if r["highlights"] else ""
        combined = f"{title_lower} {highlights_text}"

        if any(kw in combined for kw in JOB_KEYWORDS):
            filtered.append(r)
        elif company_slug in url.lower().replace(" ", "").replace("-", "").replace(".", ""):
            filtered.append(r)

    return filtered

# ---------------------------------------------------------------------------
# Email Notifications
# ---------------------------------------------------------------------------

def build_jobs_email(new_jobs):
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    by_company = {}
    for job in new_jobs:
        by_company.setdefault(job.get("company", "Unknown"), []).append(job)

    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h2 {{ color: #2c3e50; }}
        .company {{ background: #f8f9fa; padding: 10px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #3498db; }}
        .company-name {{ font-weight: bold; color: #2980b9; font-size: 16px; }}
        .job {{ margin: 5px 0 5px 15px; }}
        .job a {{ color: #333; text-decoration: none; }}
        .job a:hover {{ text-decoration: underline; }}
        .date {{ color: #7f8c8d; font-size: 12px; }}
        .footer {{ margin-top: 30px; padding-top: 10px; border-top: 1px solid #eee; color: #95a5a6; font-size: 11px; }}
    </style></head><body>
        <h2>New Job Postings Found</h2>
        <p class="date">Scanned: {date_str}</p>"""

    for company, jobs in sorted(by_company.items()):
        html += f'<div class="company"><div class="company-name">{company} ({len(jobs)} new)</div>'
        for job in jobs:
            title = (job["title"][:100] or "View Job").replace("<", "&lt;").replace(">", "&gt;")
            url = job["url"]
            pub = job.get("published_date", "")
            pub_str = f' <span class="date">({pub[:10]})</span>' if pub else ""
            html += f'<div class="job"><a href="{url}" target="_blank">{title}</a>{pub_str}</div>'
        html += "</div>"

    html += f"""<div class="footer">
            Total new listings: {len(new_jobs)} across {len(by_company)} companies<br>
            Powered by EXA Search + GitHub Actions
        </div></body></html>"""
    return html


def build_health_email(summary):
    """Build alert email for pipeline failures."""
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    failed = summary.get("companies_failed", [])

    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h2 {{ color: #e74c3c; }}
        .stat {{ margin: 5px 0; }}
        .fail {{ color: #e74c3c; }}
        .ok {{ color: #27ae60; }}
        .footer {{ margin-top: 30px; color: #95a5a6; font-size: 11px; }}
    </style></head><body>
        <h2>Job Tracker - Pipeline Alert</h2>
        <p>{date_str}</p>
        <div class="stat">Companies scanned: <b>{summary['companies_scanned']}</b></div>
        <div class="stat">New jobs found: <b class="ok">{summary['new_jobs']}</b></div>
        <div class="stat">API errors: <b class="fail">{summary['api_errors']}</b></div>
        <div class="stat">Companies failed: <b class="fail">{len(failed)}</b></div>"""

    if failed:
        html += "<h3>Failed Companies</h3><ul>"
        for f in failed:
            html += f'<li><b>{f["company"]}</b>: {f["reason"]}</li>'
        html += "</ul>"

    if summary.get("errors"):
        html += "<h3>Errors</h3><ul>"
        for e in summary["errors"]:
            html += f'<li>{e}</li>'
        html += "</ul>"

    html += """<div class="footer">Job Tracker Health Alert</div></body></html>"""
    return html


def send_email(subject, html_body, plain_body=""):
    """Send email via Gmail SMTP with retry."""
    sender = os.environ["SENDER_EMAIL"]
    password = os.environ["SENDER_PASSWORD"]
    receiver = os.environ.get("RECEIVER_EMAIL", sender)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver
    msg.attach(MIMEText(plain_body or html_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        log.info(f"Email sent to {receiver}")
        return True
    except Exception as e:
        log.error(f"Email send failed: {e}")
        return False


def send_jobs_email(new_jobs):
    if not new_jobs:
        log.info("No new jobs - skipping email")
        return
    subject = f"Job Alert: {len(new_jobs)} New Postings Found"
    html = build_jobs_email(new_jobs)
    send_email(subject, html)


def send_health_alert(summary):
    """Send health alert if there were significant errors."""
    if summary["api_errors"] == 0 and len(summary["companies_failed"]) == 0:
        return
    subject = f"Job Tracker Alert: {summary['api_errors']} API errors, {len(summary['companies_failed'])} companies failed"
    html = build_health_email(summary)
    send_email(subject, html)

# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()
    log.info(f"=== Job Tracker Harness - {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    # Pre-flight
    validate_config()

    from exa_py import Exa
    exa = Exa(api_key=os.environ["EXA_API_KEY"])

    companies = load_companies()
    seen = load_seen()
    all_new_jobs = []

    for i, company in enumerate(companies):
        name = company["name"]
        log.info(f"[{i+1}/{len(companies)}] {name}")

        try:
            raw_results = search_jobs(exa, name)
            relevant = filter_relevant(raw_results, name)

            new_for_company = []
            for r in relevant:
                jid = job_id(r["url"])
                if jid not in seen:
                    seen[jid] = {
                        "url": r["url"],
                        "title": r["title"],
                        "company": name,
                        "found_date": datetime.now().isoformat(),
                    }
                    r["company"] = name
                    new_for_company.append(r)

            if new_for_company:
                log.info(f"  -> {len(new_for_company)} new job(s)")
                all_new_jobs.extend(new_for_company)
            else:
                log.info(f"  -> 0 new jobs")

            log.company_ok(name, len(new_for_company))

        except Exception as e:
            log.company_fail(name, e)
            log.error(f"  -> FAILED: {e}")
            # Continue to next company - don't kill the run

    # Persist state
    save_seen(seen)

    elapsed = round(time.time() - start_time, 1)
    summary = log.summary()
    summary["elapsed_seconds"] = elapsed

    log.info(f"=== Done in {elapsed}s: {summary['new_jobs']} new jobs across {summary['companies_scanned']} companies ===")

    # Send notifications
    send_jobs_email(all_new_jobs)
    send_health_alert(summary)

    # Write summary for GitHub Actions
    with open("summary.txt", "w") as f:
        f.write(f"Scanned: {summary['companies_scanned']} companies\n")
        f.write(f"New jobs: {summary['new_jobs']}\n")
        f.write(f"API errors: {summary['api_errors']}\n")
        f.write(f"Companies failed: {len(summary['companies_failed'])}\n")
        f.write(f"Elapsed: {elapsed}s\n\n")
        for job in all_new_jobs:
            f.write(f"  - [{job['company']}] {job['title'][:80]} -> {job['url']}\n")

    # Exit non-zero if too many failures (but still upload state)
    failure_rate = len(summary["companies_failed"]) / max(len(companies), 1)
    if failure_rate > 0.5:
        log.error(f"FAILURE_RATE={failure_rate:.0%} - more than half the companies failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
