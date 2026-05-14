import httpx, sqlite3
rds = [r[0] for r in sqlite3.connect("tesis.db").execute("SELECT DISTINCT rd_ejecutoria FROM tesis_ejecutoria_ref LIMIT 5")]
print("sample IDs:", rds)
H = {"User-Agent":"Mozilla/5.0","Accept":"application/json","Referer":"https://bj.scjn.gob.mx/"}
for rd in rds:
    r = httpx.get(f"https://bj.scjn.gob.mx/api/v1/bj/documento/ejecutorias/{rd}", headers=H, timeout=15)
    print(f"rd={rd}: status={r.status_code} len={len(r.content)} ctype={r.headers.get('content-type','')}")
    if r.status_code == 200:
        try:
            j = r.json()
            print(f"  keys: {list(j.keys())[:12]}")
        except Exception as e:
            print(f"  parse: {e}; body[:200]: {r.text[:200]}")
