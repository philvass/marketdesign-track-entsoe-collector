#!/usr/bin/env python3
"""MarketDesign.ai TRACK V20.4 — automated external ENTSO-E collector.

Runs outside Cloudflare Workers. It discovers ENTSO-E consultations, fetches
source content, normalises it to the existing TRACK /ingest/document contract,
deduplicates by source_id + content hash, and optionally submits it.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

VERSION = "v20.4-entsoe-automation-1"
DEFAULT_TRACK_URL = "https://marketdesign-track-api.philvass.workers.dev/ingest/document"
ENTSOE_CONSULTATIONS_URL = "https://consultations.entsoe.eu/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151 Safari/537.36 "
    "MarketDesign.ai-TRACK/20.4"
)
DATE_RE = re.compile(
    r"\b(?:Opened|Closes|Closed)\s+(\d{1,2})\s+"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(20\d{2})\b",
    re.I,
)

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


@dataclass
class Candidate:
    source_id: str
    title: str
    publication_date: str | None
    url: str


class CollectorError(RuntimeError):
    pass


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Cache-Control": "no-cache",
    })
    return s


def get_with_retry(session: requests.Session, url: str, timeout: int = 30, attempts: int = 3) -> requests.Response:
    last: Exception | None = None
    for n in range(attempts):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code >= 500 and n + 1 < attempts:
                time.sleep(1.5 * (n + 1))
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last = exc
            if n + 1 < attempts:
                time.sleep(1.5 * (n + 1))
    raise CollectorError(f"GET failed for {url}: {last}")


def normalise_entsoe_date(day: str, month: str, year: str) -> str:
    return datetime(
        int(year),
        MONTHS[month.lower()],
        int(day),
    ).date().isoformat()


def source_id_from_url(url: str) -> str:
    path = url.split("?", 1)[0].strip("/").split("/")
    slug = "-".join(path[-2:]).lower()
    slug = re.sub(r"[^a-z0-9-]+", "-", slug).strip("-")
    return f"entsoe-consultation-{slug}"


def parse_candidates(html: str, base_url: str) -> list[Candidate]:
    soup = BeautifulSoup(html, "html.parser")
    found: dict[str, Candidate] = {}

    for a in soup.find_all("a", href=True):
        title = " ".join(a.get_text(" ", strip=True).split())
        url = urljoin(base_url, a["href"])

        if not title:
            continue
        if "consultations.entsoe.eu" not in url:
            continue
        if url.rstrip("/") == base_url.rstrip("/"):
            continue
        if "/consultation_finder/" in url:
            continue
        if "/user_uploads/" in url:
            continue
        if url.lower().endswith(".pdf"):
            continue

        # Consultation detail pages use paths such as /markets/ishm-assessment/.
        path_parts = [p for p in url.split("?", 1)[0].split("/") if p]
        if len(path_parts) < 2:
            continue

        sid = source_id_from_url(url)

        # Avoid duplicate menu/footer links to the same consultation.
        found.setdefault(
            sid,
            Candidate(
                source_id=sid,
                title=title,
                publication_date=None,
                url=url,
            ),
        )

    return list(found.values())


def discover(session: requests.Session) -> tuple[str, list[Candidate]]:
    r = get_with_retry(session, ENTSOE_CONSULTATIONS_URL)
    candidates = parse_candidates(r.text, r.url)
    if not candidates:
        raise CollectorError("ENTSO-E consultation discovery returned no candidates")
    return r.url, candidates


def extract_pdf_text(pdf_bytes: bytes, max_chars: int = 60000) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    parts: list[str] = []
    size = 0
    for page in reader.pages:
        text = page.extract_text() or ""
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        if not text:
            continue
        remaining = max_chars - size
        if remaining <= 0:
            break
        parts.append(text[:remaining])
        size += len(parts[-1])
    return "\n\n".join(parts).strip()


def fetch_content(session: requests.Session, candidate: Candidate) -> str:
    r = get_with_retry(session, candidate.url, timeout=45)
    content_type = (r.headers.get("content-type") or "").lower()
    if "pdf" in content_type or candidate.url.lower().endswith(".pdf"):
        text = extract_pdf_text(r.content)
    else:
        soup = BeautifulSoup(r.text, "html.parser")

        page_text = soup.get_text(" ", strip=True)
        opened = re.search(
            r"\bOpened\s+(\d{1,2})\s+"
            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(20\d{2})\b",
            page_text,
            re.I,
        )
        if opened:
            candidate.publication_date = normalise_entsoe_date(
                opened.group(1),
                opened.group(2),
                opened.group(3),
            )

        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = "\n".join(
            line.strip() for line in soup.get_text("\n").splitlines() if line.strip()
        )
        text = text[:60000]
    if len(text) < 200:
        raise CollectorError(f"Too little source text extracted from {candidate.url}")
    return text


def build_payload(candidate: Candidate, content: str) -> dict:
    return {
        "institution": "ENTSO-E",
        "document_type": "REGULATOR",
        "url": candidate.url,
        "publication_date": candidate.publication_date,
        "title": candidate.title,
        "content": content,
        "source_id": candidate.source_id,
    }


def content_hash(payload: dict) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS acquired_documents (
          source_id TEXT PRIMARY KEY,
          content_hash TEXT NOT NULL,
          source_url TEXT NOT NULL,
          publication_date TEXT,
          title TEXT NOT NULL,
          track_disposition TEXT,
          track_response_json TEXT,
          first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          submitted_at TEXT
        )
        """
    )
    db.commit()
    return db


def is_unchanged(db: sqlite3.Connection, source_id: str, digest: str) -> bool:
    row = db.execute(
        "SELECT content_hash, submitted_at FROM acquired_documents WHERE source_id=?",
        (source_id,),
    ).fetchone()
    return bool(row and row[0] == digest and row[1])


def record_seen(db: sqlite3.Connection, candidate: Candidate, digest: str) -> None:
    db.execute(
        """
        INSERT INTO acquired_documents(source_id,content_hash,source_url,publication_date,title)
        VALUES(?,?,?,?,?)
        ON CONFLICT(source_id) DO UPDATE SET
          content_hash=excluded.content_hash,
          source_url=excluded.source_url,
          publication_date=excluded.publication_date,
          title=excluded.title,
          last_seen_at=CURRENT_TIMESTAMP,
          submitted_at=CASE
            WHEN acquired_documents.content_hash=excluded.content_hash THEN acquired_documents.submitted_at
            ELSE NULL
          END
        """,
        (candidate.source_id, digest, candidate.url, candidate.publication_date, candidate.title),
    )
    db.commit()


def submit(session: requests.Session, track_url: str, payload: dict, token: str | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = session.post(track_url, json=payload, headers=headers, timeout=120)
    try:
        data = r.json()
    except ValueError:
        data = {"raw": r.text[:2000]}
    if not r.ok:
        raise CollectorError(f"TRACK returned HTTP {r.status_code}: {json.dumps(data, ensure_ascii=False)}")
    return data


def mark_submitted(db: sqlite3.Connection, source_id: str, response: dict) -> None:
    disposition = response.get("disposition") if isinstance(response, dict) else None
    db.execute(
        """
        UPDATE acquired_documents
        SET track_disposition=?, track_response_json=?, submitted_at=CURRENT_TIMESTAMP, last_seen_at=CURRENT_TIMESTAMP
        WHERE source_id=?
        """,
        (disposition, json.dumps(response, ensure_ascii=False), source_id),
    )
    db.commit()


def mark_bootstrapped(db: sqlite3.Connection, source_id: str) -> None:
    """Mark a discovered document as the automation baseline without posting to TRACK."""
    db.execute(
        """
        UPDATE acquired_documents
        SET track_disposition='BOOTSTRAPPED', submitted_at=CURRENT_TIMESTAMP, last_seen_at=CURRENT_TIMESTAMP
        WHERE source_id=?
        """,
        (source_id,),
    )
    db.commit()


def choose(candidates: Iterable[Candidate], match: str | None, limit: int) -> list[Candidate]:
    items = list(candidates)
    if match:
        needle = match.lower()
        items = [c for c in items if needle in c.title.lower() or needle in c.source_id.lower()]
    # Listing is normally newest-first. Date sort makes it explicit when dates parse.
    items.sort(key=lambda c: (c.publication_date or "0000-00-00", c.source_id), reverse=True)
    return items[:limit]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="MarketDesign.ai TRACK V20.4 automated external ENTSO-E collector")
    p.add_argument("--submit", action="store_true", help="POST new/changed documents into TRACK")
    p.add_argument("--dry-run", action="store_true", help="Discover + fetch + normalise, but never submit")
    p.add_argument("--bootstrap-state", action="store_true", help="Record current documents as the baseline without submitting them")
    p.add_argument("--limit", type=int, default=1, help="Maximum decisions to process (default: 1)")
    p.add_argument("--match", help="Only process candidates whose title/source_id contains this text")
    p.add_argument("--track-url", default=os.getenv("TRACK_INGEST_URL", DEFAULT_TRACK_URL))
    p.add_argument("--token", default=os.getenv("TRACK_INGEST_TOKEN"))
    p.add_argument("--state", default=os.getenv("COLLECTOR_STATE", "./state/entsoe.sqlite3"))
    p.add_argument("--json", action="store_true", help="Emit machine-readable summary JSON")
    args = p.parse_args(argv)

    selected_modes = sum(bool(x) for x in (args.submit, args.dry_run, args.bootstrap_state))
    if selected_modes > 1:
        p.error("choose only one of --submit, --dry-run, or --bootstrap-state")
    if selected_modes == 0:
        args.dry_run = True

    session = make_session()
    db = init_db(Path(args.state))
    source_url, discovered = discover(session)
    selected = choose(discovered, args.match, max(1, args.limit))
    if not selected:
        raise CollectorError(f"No ENTSO-E consultation matched {args.match!r}")

    results = []
    for candidate in selected:
        content = fetch_content(session, candidate)
        payload = build_payload(candidate, content)
        digest = content_hash(payload)
        duplicate = is_unchanged(db, candidate.source_id, digest)
        record_seen(db, candidate, digest)
        item = {
            "candidate": asdict(candidate),
            "content_chars": len(content),
            "content_hash": digest,
            "duplicate": duplicate,
            "submitted": False,
            "track_response": None,
        }
        if args.bootstrap_state:
            if not duplicate:
                mark_bootstrapped(db, candidate.source_id)
            item["bootstrapped"] = not duplicate
        elif args.submit and not duplicate:
            response = submit(session, args.track_url, payload, args.token)
            mark_submitted(db, candidate.source_id, response)
            item["submitted"] = True
            item["track_response"] = response
        results.append(item)

    summary = {
        "ok": True,
        "collector_version": VERSION,
        "mode": "BOOTSTRAP" if args.bootstrap_state else ("SUBMIT" if args.submit else "DRY_RUN"),
        "discovery_source": source_url,
        "discovered_count": len(discovered),
        "processed_count": len(results),
        "results": results,
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"{VERSION} | {summary['mode']}")
        print(f"Discovery: {source_url}")
        print(f"Discovered: {len(discovered)} | Processed: {len(results)}")
        for r in results:
            c = r["candidate"]
            print(f"- {c['source_id']} | {c['publication_date']} | {c['title']}")
            print(f"  source: {c['url']}")
            print(f"  chars: {r['content_chars']} | hash: {r['content_hash'][:16]}… | duplicate: {r['duplicate']}")
            if r["submitted"]:
                print(f"  TRACK: {json.dumps(r['track_response'], ensure_ascii=False)}")
        if args.bootstrap_state:
            print("BOOTSTRAP: baseline recorded locally; nothing was submitted to TRACK.")
        elif args.dry_run:
            print("DRY RUN: nothing was submitted to TRACK.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CollectorError as exc:
        print(json.dumps({"ok": False, "collector_version": VERSION, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2)
