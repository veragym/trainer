import os, csv, json, requests
from datetime import datetime, timezone

SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

HEADERS = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
}

TABLES = ["trainers","members","exercises_lib","exercise_refs","pt_sessions","session_exs"]
TODAY  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
OUT_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(OUT_DIR, exist_ok=True)

def fetch_all(table):
    rows, offset, limit = [], 0, 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=*&offset={offset}&limit={limit}"
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        batch = r.json()
        if not batch: break
        rows.extend(batch)
        if len(batch) < limit: break
        offset += limit
    return rows

def save_csv(table, rows):
    if not rows:
        print(f"  {table}: 데이터 없음")
        return
    path = os.path.join(OUT_DIR, f"{table}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {table}: {len(rows)}행 저장")

def save_summary(summary):
    path = os.path.join(OUT_DIR, "backup_log.json")
    logs = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            try: logs = json.load(f)
            except: logs = []
    logs.append(summary)
    logs = logs[-30:]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(logs, f, ensure_ascii=False, indent=2)

def main():
    print(f"\n베라짐 자동 백업 — {TODAY}\n")
    summary = {"date": TODAY, "tables": {}, "status": "success"}
    for table in TABLES:
        try:
            rows = fetch_all(table)
            save_csv(table, rows)
            summary["tables"][table] = len(rows)
        except Exception as e:
            print(f"  {table} 실패: {e}")
            summary["tables"][table] = f"ERROR: {e}"
            summary["status"] = "partial"
    save_summary(summary)
    print("\n백업 완료!")

if __name__ == "__main__":
    main()
