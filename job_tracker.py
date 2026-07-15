import json
import os
import hashlib
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
from exa_py import Exa

# --- Config ---
EXA_API_KEY = os.environ["EXA_API_KEY"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
SENDER_PASSWORD = os.environ["SENDER_PASSWORD"]  # Gmail App Password
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL", SENDER_EMAIL)

STATE_FILE = Path("seen_jobs.json")
COMPANIES_FILE = Path("companies.json")

# Search queries - each company gets two searches: Pakistan-specific + remote
SEARCH_TEMPLATES = [
    "{company} hiring Pakistan software engineer jobs",
    "{company} remote software engineer jobs 2025",
]


def load_companies():
    with open(COMPANIES_FILE, "r") as f:
        data = json.load(f)
    return data["companies"]


def load_seen():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_seen(seen):
    with open(STATE_FILE, "w") as f:
        json.dump(seen, f, indent=2)


def job_id(url):
    return hashlib.md5(url.encode()).hexdigest()


def search_jobs(exa_client, company_name):
    """Search EXA for job postings at a company."""
    results = []
    for template in SEARCH_TEMPLATES:
        query = template.format(company=company_name)
        try:
            response = exa_client.search(
                query,
                type="auto",
                num_results=10,
                contents={"highlights": True},
            )
            for r in response.results:
                results.append({
                    "title": r.title or "",
                    "url": r.url,
                    "published_date": str(r.published_date) if r.published_date else "",
                    "highlights": r.highlights if r.highlights else [],
                })
            # Rate limit: EXA has limits, be nice
            time.sleep(1)
        except Exception as e:
            print(f"  [ERROR] Searching '{query}': {e}")
            time.sleep(2)
    return results


def filter_relevant(results, company_name):
    """Basic dedup by URL and filter out obvious non-jobs."""
    seen_urls = set()
    filtered = []
    keywords = ["job", "career", "hiring", "position", "apply", "opening",
                "vacancy", "recruit", "engineer", "developer", "remote"]

    for r in results:
        url = r["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)

        title_lower = r["title"].lower()
        highlights_text = " ".join(r["highlights"]).lower() if r["highlights"] else ""
        combined = title_lower + " " + highlights_text

        # Keep if it looks job-related
        if any(kw in combined for kw in keywords):
            filtered.append(r)
        # Also keep company career pages
        elif company_name.lower().replace(" ", "") in url.lower().replace(" ", ""):
            filtered.append(r)

    return filtered


def build_email_body(new_jobs):
    """Build HTML email with new job listings."""
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h2 {{ color: #2c3e50; }}
        .company {{ background: #f8f9fa; padding: 10px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #3498db; }}
        .company-name {{ font-weight: bold; color: #2980b9; font-size: 16px; }}
        .job {{ margin: 5px 0 5px 15px; }}
        .job a {{ color: #333; text-decoration: none; }}
        .job a:hover {{ text-decoration: underline; }}
        .date {{ color: #7f8c8d; font-size: 12px; }}
        .footer {{ margin-top: 30px; padding-top: 10px; border-top: 1px solid #eee; color: #95a5a6; font-size: 11px; }}
    </style>
    </head>
    <body>
        <h2>New Job Postings Found</h2>
        <p class="date">Scanned: {date_str}</p>
    """

    # Group by company
    by_company = {}
    for job in new_jobs:
        company = job.get("company", "Unknown")
        by_company.setdefault(company, []).append(job)

    for company, jobs in sorted(by_company.items()):
        html += f'<div class="company"><div class="company-name">{company} ({len(jobs)} new)</div>'
        for job in jobs:
            title = job["title"][:100] if job["title"] else "View Job"
            url = job["url"]
            pub = job.get("published_date", "")
            pub_str = f' <span class="date">({pub[:10]})</span>' if pub else ""
            html += f'<div class="job"><a href="{url}" target="_blank">{title}</a>{pub_str}</div>'
        html += "</div>"

    html += f"""
        <div class="footer">
            Total new listings: {len(new_jobs)} across {len(by_company)} companies<br>
            Powered by EXA Search + GitHub Actions
        </div>
    </body>
    </html>
    """
    return html


def send_email(new_jobs):
    """Send email notification via Gmail SMTP."""
    if not new_jobs:
        print("No new jobs to notify about.")
        return

    html = build_email_body(new_jobs)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Job Alert: {len(new_jobs)} New Postings Found"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    plain_text = f"Found {len(new_jobs)} new job postings. Open in a browser to see details."
    msg.attach(MIMEText(plain_text, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        print(f"Email sent to {RECEIVER_EMAIL}")
    except Exception as e:
        print(f"Failed to send email: {e}")


def main():
    print(f"=== Job Tracker - {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    companies = load_companies()
    seen = load_seen()
    exa = Exa(api_key=EXA_API_KEY)

    all_new_jobs = []

    for i, company in enumerate(companies):
        name = company["name"]
        print(f"[{i+1}/{len(companies)}] Searching: {name}")

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
            print(f"  -> {len(new_for_company)} new job(s) found")
            all_new_jobs.extend(new_for_company)
        else:
            print(f"  -> No new jobs")

    save_seen(seen)

    print(f"\n=== Summary: {len(all_new_jobs)} new job(s) total ===")

    if all_new_jobs:
        send_email(all_new_jobs)

    # Write summary for GitHub Actions
    summary_file = Path("summary.txt")
    with open(summary_file, "w") as f:
        f.write(f"Scanned {len(companies)} companies\n")
        f.write(f"Found {len(all_new_jobs)} new job(s)\n")
        for job in all_new_jobs:
            f.write(f"  - [{job['company']}] {job['title'][:80]} -> {job['url']}\n")


if __name__ == "__main__":
    main()
