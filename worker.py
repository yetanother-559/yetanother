import requests
from bs4 import BeautifulSoup as bs
import html
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

SERVER = "https://test.avighnac.me"
MAX_WORKERS = 32                  
BASE = "https://oj.uz/submission/{}"

def clean_int(s: str) -> int:
    m = re.search(r"\d+", s or "")
    return int(m.group(0)) if m else 0

def clean_float(s: str) -> float:
    m = re.search(r"\d+(?:\.\d+)?", s or "")
    return float(m.group(0)) if m else 0.0

def fetch_one(i: int):
    """Fetch and parse one submission page. Returns row tuple or None/'NOTFOUND'."""
    url = BASE.format(i)
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            print(f"[INFO] {i} is 404, skipping")
            return "NOTFOUND"
        if r.status_code != 200:
            print(f"[WARN] {i} returned status {r.status_code}")
            return None

        soup = bs(r.text, "html.parser")

        # code
        div = soup.find("div", id="submissionCode-0")
        code = html.unescape(div.decode_contents()) if div else ""

        # metadata
        table = soup.find("table", class_="table table-striped table-condensed")
        if not table or not table.tbody or not table.tbody.tr:
            print(f"[ERROR] {i} metadata missing")
            return None

        row = table.tbody.tr
        links = row.find_all("a")
        username = links[0].get_text(strip=True) if len(links) >= 1 else ""
        problem_link = "https://oj.uz" + links[1]["href"] if len(links) >= 2 else ""

        tspan = row.find("span", {"data-format": "datetime"})
        ts_iso = tspan.get("data-timestamp-iso") if tspan else ""

        tds = row.find_all("td")
        language = tds[4].get_text(strip=True) if len(tds) >= 5 else ""

        score_text = row.find("div", class_="text")
        score = clean_float(score_text.get_text(strip=True)) if score_text else 0.0

        exec_span = row.find("span", id=lambda x: x and x.startswith("submission_max_execution_time"))
        mem_span  = row.find("span", id=lambda x: x and x.startswith("submission_max_memory"))
        execution_time = clean_int(exec_span.get_text(strip=True) if exec_span else "")
        memory        = clean_int(mem_span.get_text(strip=True) if mem_span else "")

        subs = []
        for sp in soup.find_all("span", class_="subtask-score"):
            subs.append(clean_float(sp.get_text(strip=True)))
        subtask_scores = "[" + ",".join(str(s) for s in subs) + "]"

        print(f"[DEBUG] Parsed submission {i}: user={username}, problem={problem_link}, lang={language}, score={score}")
        return (i, ts_iso, username, problem_link, language, float(score),
                subtask_scores, int(execution_time), int(memory), code)

    except Exception as e:
        print(f"[ERROR] Exception fetching {i}: {e}")
        return None

def safe_request(method, url, **kwargs):
    """Wrapper around requests that retries forever if server is down."""
    delay = 5
    while True:
        try:
            r = requests.request(method, url, timeout=30, **kwargs)
            return r
        except Exception as e:
            print(f"[ERROR] {method.upper()} {url} failed: {e} — retrying in {delay}s")
            time.sleep(delay)

def main():
    while True:
        # ask server for work
        r = safe_request("GET", f"{SERVER}/get_work")
        try:
            ids = r.json().get("ids", [])
        except Exception as e:
            print(f"[ERROR] Failed to decode /get_work JSON: {e}")
            time.sleep(5)
            continue

        if not ids:
            print("[INFO] No work available, sleeping...")
            time.sleep(5)
            continue

        print(f"[INFO] Got {len(ids)} IDs from server")

        results = []
        notfound = []

        # scrape concurrently
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(fetch_one, i): i for i in ids}
            for fut in as_completed(futures):
                i = futures[fut]
                row = fut.result()
                if row == "NOTFOUND":
                    notfound.append(i)
                elif row:
                    results.append(row)

        # send back to server (with retry)
        payload = {"submissions": results, "notfound": notfound}
        r = safe_request("POST", f"{SERVER}/submit_work", json=payload)
        try:
            print(f"[INFO] Submitted batch: {r.json()}")
        except Exception:
            print(f"[WARN] Submitted batch but got invalid response")

if __name__ == "__main__":
    main()
