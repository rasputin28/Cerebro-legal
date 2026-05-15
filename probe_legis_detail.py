"""Find the detail endpoint for legislacion (with full text/url)."""
import httpx, sqlite3, json

H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json",
     "Referer": "https://bj.scjn.gob.mx/"}

c = sqlite3.connect("legislacion.db")
r = c.execute("""
    SELECT id_native, raw_json FROM legislacion
    WHERE ambito='ESTATAL' AND estado='JALISCO'
    AND ordenamiento LIKE 'CODIGO CIVIL%' AND vigencia='VIGENTE' LIMIT 1
""").fetchone()
sid = r[0]
print("sample id:", sid)
print("raw_json keys:", list(json.loads(r[1]).keys())[:20])

# Try various detail patterns
for path in [
    f"/documento/legislacion/{sid}",
    f"/documento/legislaciones/{sid}",
    f"/legislacion/{sid}",
    f"/storage/legislacion?externo=true&fileparams=filename:{sid}",
]:
    try:
        rr = httpx.get(f"https://bj.scjn.gob.mx/api/v1/bj{path}", headers=H, timeout=15)
        ct = rr.headers.get("content-type", "")[:30]
        print(f"  {path}: {rr.status_code} len={len(rr.content)} ctype={ct}")
        if rr.status_code == 200 and len(rr.content) > 500:
            try:
                j = rr.json()
                print(f"    keys: {list(j.keys())[:20]}")
            except Exception:
                print(f"    body[:300]: {rr.text[:300]}")
    except Exception as e:
        print(f"  {path}: ERR {e}")

# Also look at the raw_json we already have — maybe URL is there
import json as j_mod
full = j_mod.loads(r[1])
print("\nFull listing payload:")
for k, v in full.items():
    s = str(v)[:120]
    print(f"  {k}: {s}")
