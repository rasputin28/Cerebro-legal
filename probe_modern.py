"""
Probe whether the existing /historicalfile endpoint (or a sibling) returns
tesis modernas — the ones that should carry urlSemanario + precedentes.

Strategy: try the same endpoint with all idEpoca values 5-13 (modern epochs),
and try the sibling path /tesisfile / /tesis (without /historicalfile/).
"""
import json
import httpx

BASE = "https://sjf2.scjn.gob.mx/services/sjftesismicroservice/api/public"
H = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-MX,es;q=0.9",
    "Origin": "https://sjf2.scjn.gob.mx",
    "Referer": "https://sjf2.scjn.gob.mx/listado-tesis",
    "Content-Type": "application/json",
}

def body(epocas=None, search_terms=None):
    return {
        "classifiers": [
            {"name": "idEpoca", "value": epocas or [], "allSelected": not epocas, "visible": False, "isMatrix": True},
        ],
        "searchTerms": search_terms or [],
        "bFacet": True,
        "ius": [],
        "idApp": "SJFAPP2020",
        "lbSearch": ["Todo"],
        "filterExpression": "",
    }

candidates = [
    ("historicalfile/all-epochs", "/historicalfile", body()),
    ("file/all-epochs", "/file", body()),
    ("tesisfile/all-epochs", "/tesisfile", body()),
    ("ejecutoria/all", "/ejecutoria", body()),
    ("sentencia/all", "/sentencia", body()),
]

# epoch IDs 5..13 to enumerate modern epochs via historicalfile
for ep in range(5, 14):
    candidates.append((f"historicalfile/idEpoca={ep}", "/historicalfile", body([str(ep)])))

with httpx.Client(http2=True, timeout=30, headers=H) as c:
    for label, path, b in candidates:
        url = BASE + path
        try:
            r = c.post(url, params={"page": 0, "size": 1}, json=b)
            tag = f"{r.status_code} len={len(r.content)}"
            if r.status_code == 200:
                try:
                    j = r.json()
                    total = j.get("total")
                    docs = j.get("documents", [])
                    first_keys = list(docs[0].keys())[:6] if docs else []
                    has_url = bool(docs and docs[0].get("urlSemanario"))
                    has_prec = bool(docs and docs[0].get("precedentes"))
                    sample_epoca = docs[0].get("epoca") if docs else None
                    tag += f" total={total} url={has_url} prec={has_prec} epoca={sample_epoca!r}"
                except Exception as e:
                    tag += f" parse_err={e}"
            print(f"{label:40s}  {tag}")
        except Exception as e:
            print(f"{label:40s}  ERR {e}")
