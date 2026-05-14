"""
Deep probe of bj.scjn `tesis` index.

Confirm:
1. Épocas coverage (5-11) via facets.
2. urlSemanario + precedentes populated in modern docs.
3. tesis detail/storage endpoint.
4. Pagination cap (size=50, page=200 == offset 10k).
5. Counts per época for slicing strategy.
6. Filter taxonomy (instancia, materia, organoJurisdiccional, tipo).
"""
import json, httpx, time
from pathlib import Path

BJ = "https://bj.scjn.gob.mx/api/v1/bj"
H = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-MX,es;q=0.9",
    "Origin": "https://bj.scjn.gob.mx",
    "Referer": "https://bj.scjn.gob.mx/",
    "Content-Type": "application/json",
}


def body(q="*", page=1, size=10, filtros=None):
    return {
        "q": q, "page": page, "size": size,
        "indice": "tesis", "fuente": None,
        "extractos": 200, "semantica": 0,
        "filtros": filtros or {},
        "sortField": "", "sortDireccion": "",
    }


with httpx.Client(http2=True, timeout=60, headers=H) as c:
    # 1. Get a wide search response and inspect facets/aggs.
    print("=== q='*' baseline ===")
    r = c.post(f"{BJ}/busqueda", json=body(size=50))
    j = r.json()
    print(f"  total: {j.get('total')}")
    print(f"  pages: {j.get('totalPaginas')}")
    top_keys = list(j.keys())
    print(f"  top-level keys: {top_keys}")
    # facets often live under 'agregaciones', 'facetas', 'filtros', 'aggs'
    for k in top_keys:
        if k != "resultados":
            v = j[k]
            preview = json.dumps(v, ensure_ascii=False)[:400] if not isinstance(v, str) else v[:400]
            print(f"  {k}: {preview}")

    # 2. Inspect a modern (Décima or Undécima Época) doc.
    print("\n=== sample modern doc keys ===")
    docs = j["resultados"]
    print(f"got {len(docs)} docs")
    # Try to find one from 11ª época
    for ep_num in ["11", "10", "9"]:
        cand = [d for d in docs if d.get("epoca", {}).get("numero") == ep_num]
        if cand:
            d = cand[0]
            print(f"\n--- Época {ep_num} sample (registroDigital={d.get('registroDigital')}) ---")
            for k, v in d.items():
                s = json.dumps(v, ensure_ascii=False)[:160] if not isinstance(v, str) else v[:160]
                print(f"  {k}: {s}")
            break

    # 3. Try fetching FULL detail to look for urlSemanario + precedentes.
    print("\n=== try detail endpoints for tesis ===")
    sample_doc = docs[0]
    rd = sample_doc.get("registroDigital")
    for path in [
        f"/tesis/{rd}",
        f"/tesis/detalle/{rd}",
        f"/documento/tesis/{rd}",
        f"/storage/tesis?externo=true&fileparams=filename:{rd}",
        f"/detalle/tesis/{rd}",
    ]:
        try:
            r = c.get(BJ + path, timeout=15)
            if r.status_code < 400:
                print(f"  HIT {path}: {r.status_code} ctype={r.headers.get('content-type','')} len={len(r.content)}")
                print(f"  body[:300]: {r.text[:300]}")
        except Exception as e:
            print(f"  ERR {path}: {e}")

    # 4. Pagination cap test.
    print("\n=== pagination cap probe ===")
    for page in [199, 200, 201, 300]:
        try:
            r = c.post(f"{BJ}/busqueda", json=body(size=50, page=page), timeout=30)
            n = len(r.json().get("resultados", [])) if r.status_code == 200 else 0
            print(f"  page={page} (offset {(page-1)*50}): status={r.status_code} got={n}")
        except Exception as e:
            print(f"  page={page}: ERR {e}")

    # 5. Per-época counts via facet (try setting filter and counting total).
    print("\n=== per-época totals (via filtros) ===")
    # First, find the filter key. Try common names.
    epoca_filter_names = ["epoca", "idEpoca", "epoca.numero", "epoca.nombre", "Época"]
    for fname in epoca_filter_names:
        # Try with one value to see if filter is accepted
        b = body(size=1, filtros={fname: ["10"]})
        r = c.post(f"{BJ}/busqueda", json=b)
        t = r.json().get("total") if r.status_code == 200 else None
        print(f"  filtros[{fname}]=['10'] -> total={t}")
        if t and t > 0 and t != 311364:
            print(f"     ^ that key works")
            # Now enumerate
            for ep in ["1","2","3","4","5","6","7","8","9","10","11"]:
                b2 = body(size=1, filtros={fname: [ep]})
                r2 = c.post(f"{BJ}/busqueda", json=b2)
                if r2.status_code == 200:
                    print(f"     época {ep}: total={r2.json().get('total')}")
            break

    # 6. Search by época nombre instead
    print("\n=== época via nombre ===")
    for nombre in ["Décima Época", "Undécima Época", "Quinta Época"]:
        b = body(size=1, filtros={"epoca": [nombre]})
        r = c.post(f"{BJ}/busqueda", json=b)
        print(f"  {nombre}: status={r.status_code} total={r.json().get('total') if r.status_code==200 else r.text[:100]}")

    # 7. Try urlSemanario / precedentes specifically — search in the sample doc.
    print("\n=== urlSemanario + precedentes presence ===")
    # We need to look in either the listing or a detail call.
    found_url = sum(1 for d in docs if d.get("urlSemanario") or any("url" in k.lower() for k in d.keys()))
    print(f"  docs with url-ish key: {found_url}/{len(docs)}")
    print(f"  all keys in first doc: {sorted(docs[0].keys())}")
