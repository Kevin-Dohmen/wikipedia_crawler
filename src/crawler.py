import psycopg2 as pg
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import threading

import url_queries as uq
import os
import time

# config

skip_non_html = True
domain_filter_regex = r'https?://([a-zA-Z0-9-]+\.)*wikipedia\.[a-z]+(/.*)?'
debug = True

db_config = {
    "dbname": os.getenv("DB_NAME", "wikipedia_crawler"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

gcon = pg.connect(
    dbname=db_config["dbname"],
    user=db_config["user"],
    password=db_config["password"],
    host=db_config["host"],
    port=db_config["port"]
)

gcur = gcon.cursor()

sites = []

def extract_urls(text, base_url: str | None = None) -> list[str]:
    urls = set()

    def trim_trailing_punct(s: str) -> str:
        # characters we're generally OK to strip if extraneous
        simple_strip = set('.,;:!?"\'<>')
        # closing->opening pairs we need to balance
        closers = {')': '(', ']': '[', '}': '{'}
        while s:
            last = s[-1]
            if last in simple_strip:
                s = s[:-1]
                continue
            if last in closers:
                opener = closers[last]
                # only strip the closer if there are more closers than openers
                if s.count(last) > s.count(opener):
                    s = s[:-1]
                    continue
                # balanced - keep it (it's probably part of the path)
            break
        return s

    def normalize(raw: str) -> str | None:
        if not raw:
            return None
        raw = raw.strip()
        if raw.startswith('#'):
            return None
        # strip common trailing punctuation carefully (preserve balanced parentheses/brackets)
        raw = trim_trailing_punct(raw)
        # protocol-relative (//host/path)
        if raw.startswith('//'):
            if base_url:
                scheme = urlparse(base_url).scheme or 'https'
                resolved = f"{scheme}:{raw}"
            else:
                resolved = f"https:{raw}"
        # absolute http(s)
        elif raw.startswith('http://') or raw.startswith('https://'):
            resolved = raw
        # other schemes we don't want
        elif raw.startswith(('mailto:', 'javascript:', 'data:')):
            return None
        # relative path -> resolve with base if available
        else:
            if base_url:
                resolved = urljoin(base_url, raw)
            else:
                return None

        # final trim after resolution (sometimes join adds punctuation back)
        resolved = trim_trailing_punct(resolved)

        p = urlparse(resolved)
        if p.scheme in ('http', 'https') and p.netloc:
            return resolved
        return None

    try:
        soup = BeautifulSoup(text, "html.parser")
        for tag in soup.find_all(href=True):
            n = normalize(tag["href"])
            if n:
                urls.add(n)
        for tag in soup.find_all(src=True):
            n = normalize(tag["src"])
            if n:
                urls.add(n)
    except Exception:
        pass

    # regex fallback for plain text absolute URLs
    url_pattern = r'\bhttps?://[^\s"\'<>)]+'  # still conservative; normalize() will re-trim safely
    for m in re.findall(url_pattern, text):
        n = normalize(m)
        if n:
            urls.add(n)
    
    if domain_filter_regex:
        pattern = re.compile(domain_filter_regex)
        urls = {u for u in urls if pattern.match(u)}

    cleaned = set()
    for u in urls:
        p = urlparse(u)
        if p.fragment:
            p = p._replace(fragment='')
        cleaned.add(p.geturl())
    urls = cleaned

    return list(urls)

def scrape_website(url: str) -> str:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        if skip_non_html and 'text/html' not in response.headers.get('Content-Type', ''):
            print(f"Skipping non-HTML content at {url}")
            return ""

        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        raise requests.ConnectionError from e
    

def scrape_step(site: uq.urlModel, cur, con):
    if not site:
        print("No URLs to process.")
        return
    
    print(f"Processing URL: {site.url}")

    uq.mark_url_as_scanning(site.id, cur, con)

    try:
        content = scrape_website(site.url)

        added = 0

        if content:

            urls = extract_urls(content, base_url=site.url)
            new_url_models = uq.add_urls(urls, cur, con)
            added = len(new_url_models)
            uq.add_url_relations(site.id, [u.id for u in new_url_models], cur, con)
                
            con.commit()

            uq.mark_url_as_scanned(site.id, cur, con)

        else:
            uq.mark_url_as_scanned(site.id, cur, con)
    except requests.ConnectionError:
        uq.url_set_error(site.id, cur, con)
        uq.mark_url_as_unscanned(site.id, cur, con)
    except Exception as e:
        print(f"Error processing {site.url}: {e}")
        uq.mark_url_as_unscanned(site.id, cur, con)

    if debug:
        cur.execute("SELECT COUNT(*) FROM found_urls")
        total_urls = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM found_urls WHERE status = TRUE")
        scanned_urls = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM url_relations")
        total_relations = cur.fetchone()[0]

        print(f"Finished processing {site.url}.\nAdded {added} new URLs.\nTotal URLs found: {total_urls}.\nTotal scanned URLs: {scanned_urls}.\nTotal URL relations: {total_relations}.\n")

def worker_loop(cur, con):
    while True:
        try:
            if sites:
                site = sites.pop(0)
                if site is None:
                    print("No URLs to process. Waiting...")
                    time.sleep(5)
                    continue
                scrape_step(site, cur, con)
            else:
                # no work currently — avoid busy loop
                time.sleep(1)
        except Exception as e:
            print(f"Error in worker loop: {e}")
            time.sleep(5)

def main():
    # reset any stuck "scanning" rows left from a previous run
    try:
        gcur.execute("UPDATE found_urls SET status = NULL WHERE status = FALSE")
        gcon.commit()
    except Exception as e:
        print(f"Warning: failed to reset stuck scan flags: {e}")
        try:
            gcon.rollback()
        except Exception:
            pass

    # start workers
    for _ in range(4):
        # each worker gets its own cursor and connection
        wcon = pg.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        wcur = wcon.cursor()
        t = threading.Thread(target=worker_loop, args=(wcur, wcon), daemon=True)
        t.start()

    global sites

    # refill sites list loop
    while True:
        try:
            if len(sites) < 50:
                unscanned: list[uq.urlModel] = uq.get_unscanned_urls(gcur)
                if not unscanned:
                    # no work in DB right now — wait and poll again
                    print("No unscanned URLs found in DB. Sleeping...")
                    time.sleep(10)
                    continue
                new = [u for u in unscanned if u.id not in {s.id for s in sites}]
                sites.extend(new)

            if not sites:
                print("No URLs to process. Waiting...")
                time.sleep(10)
                continue

            time.sleep(5)
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(10)

if __name__ == "__main__":  
    main()

# largely written by copilot (i'm lazy)
