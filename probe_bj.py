"""
Deep-probe bj.scjn.gob.mx:
- Total sentencias (fuente=3 / indice=sentencias_pub)
- Document shape and identifiers
- Try detail endpoints to find ONE that returns the full sentencia
- Try PDF endpoints (engrose is the official PDF)
"""
import json, httpx

BJ = "https://bj.scjn.gob.mx/api/v1/bj"
H = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-MX,es;q=0.9",
    "Origin": "https://bj.scjn.gob.mx",
    "Referer": "https://bj.scjn.gob.mx/",
    "Content-Type": "application/json",
}

def q_body(indice=None, q="*", page=1, size=10, filtros=None):
    return {
        "q": q,
        "page": page,
        "size": size,
        "indice": indice,
        "fuente": None,
        "extractos": 200,
        "semantica": 0,
        "filtros": filtros or {},
        "sortField": "",
        "sortDireccion": "",
    }

c = httpx.Client(http2=True, timeout=45, headers=H, follow_redirects=True)

# 1. List ALL sources (full dump)
r = c.get(f"{BJ}/fuentes?group=true")
print("FUENTES:")
print(json.dumps(r.json(), ensure_ascii=False, indent=2)[:3000])
print()

# 2. Empty/wildcard sentencias to get total + shape
print("=== sentencias_pub with q='*' ===")
r = c.post(f"{BJ}/busqueda", json=q_body(indice="sentencias_pub", q="*"))
print("status:", r.status_code)
j = r.json()
print("total sentencias:", j.get("total"), "totalPaginas:", j.get("totalPaginas"))
docs = j.get("resultados", [])
print(f"got {len(docs)} sample docs; keys of first:")
if docs:
    for k, v in docs[0].items():
        s = json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v
        print(f"  {k:25s} = {s[:120]}")

# Save 3 sample IDs for downstream probes
SAMPLES = docs[:3]

# 3. Try empty q variants — many APIs reject "*"
for q_try in ["", "amparo", "matchall"]:
    r = c.post(f"{BJ}/busqueda", json=q_body(indice="sentencias_pub", q=q_try))
    if r.status_code == 200:
        print(f"q={q_try!r}: total={r.json().get('total')}")

# 4. Try detail endpoints with the first sample's identifier
print("\n=== probe detail endpoints ===")
if SAMPLES:
    s0 = SAMPLES[0]
    # Likely identifier fields by source defn was identificador=idEngrose
    cand_ids = {k: v for k, v in s0.items() if "id" in k.lower() and v not in (None, "", 0)}
    print("identifier candidates from sample:", cand_ids)
    for k, v in cand_ids.items():
        for path in [
            f"/sentencia/{v}",
            f"/sentencias/{v}",
            f"/sentencias_pub/{v}",
            f"/documento/{v}",
            f"/detalle/{v}",
            f"/engrose/{v}",
            f"/sentencia/by-id/{v}",
        ]:
            try:
                rr = c.get(BJ + path)
                if rr.status_code != 404:
                    print(f"  HIT {path} -> {rr.status_code} len={len(rr.content)} ctype={rr.headers.get('content-type','')}")
            except Exception as e:
                print(f"  ERR {path}: {e}")

# 5. Try expediente lookup
print("\n=== expedientes_pub ===")
r = c.post(f"{BJ}/busqueda", json=q_body(indice="expedientes_pub", q="*"))
print("status:", r.status_code, "total:", r.json().get("total") if r.status_code == 200 else r.text[:200])

# 6. Try tesis fuente — these are the modern ones with urlSemanario
print("\n=== tesis (modern, all epocas) ===")
r = c.post(f"{BJ}/busqueda", json=q_body(indice="tesis", q="*"))
if r.status_code == 200:
    j = r.json()
    print("total tesis:", j.get("total"))
    if j["resultados"]:
        d = j["resultados"][0]
        print("first keys:", list(d.keys())[:15])
        print("epoca:", d.get("epoca"))
        # Check for urlSemanario or similar
        for k in d.keys():
            if "url" in k.lower() or "prec" in k.lower() or "regis" in k.lower():
                print(f"  {k}: {str(d[k])[:120]}")
