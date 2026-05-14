"""
Find where archivoURL files are served from + pilot download of 10 sentencias.
"""
import json, httpx, os, time
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

OUT_DIR = Path(__file__).parent / "sentencias_pilot"
OUT_DIR.mkdir(exist_ok=True)

c = httpx.Client(http2=True, timeout=60, headers=H, follow_redirects=True)

# 1. Pull 10 diverse sentencias from various años / organos.
all_docs = []
for year_filter in [{}, {"anio": ["2024"]}, {"anio": ["2010"]}, {"anio": ["2000"]}, {"anio": ["1990"]}]:
    body = {
        "q": "*", "page": 1, "size": 3,
        "indice": "sentencias_pub", "fuente": None,
        "extractos": 200, "semantica": 0,
        "filtros": year_filter,
        "sortField": "", "sortDireccion": "",
    }
    r = c.post(f"{BJ}/busqueda", json=body)
    if r.status_code == 200:
        docs = r.json().get("resultados", [])
        all_docs.extend(docs)
        print(f"filter={year_filter}: got {len(docs)} (total in filter: {r.json().get('total')})")

# Dedup by idEngrose
seen = set()
samples = []
for d in all_docs:
    if d.get("idEngrose") not in seen:
        seen.add(d.get("idEngrose"))
        samples.append(d)
samples = samples[:10]
print(f"\nUnique pilot set: {len(samples)} sentencias")

# 2. For each sample, dump metadata + try to find the file.
HOST_CANDS = [
    "https://bj.scjn.gob.mx/doc/sentencias_pub/{a}",
    "https://bj.scjn.gob.mx/sentencias/{a}",
    "https://bj.scjn.gob.mx/static/sentencias/{a}",
    "https://bj.scjn.gob.mx/files/sentencias_pub/{a}",
    "https://bj.scjn.gob.mx/api/v1/bj/sentencia/{ie}/archivo",
    "https://bj.scjn.gob.mx/api/v1/bj/archivo/sentencias_pub/{ie}",
    "https://bj.scjn.gob.mx/api/v1/bj/documento/{ie}",
    # Likely the real path — SIJ static host
    "https://www.scjn.gob.mx/sites/default/files/listas_sentencias_resueltas/documento/{a}",
    "https://www2.scjn.gob.mx/ConsultaTematica/PaginasPub/DetallePub.aspx?AsuntoID={aid}",
]

results = []
for s in samples:
    arch = s.get("archivoURL", "")
    ie = s.get("idEngrose")
    aid = s.get("asuntoID")
    print(f"\nidEngrose={ie} asuntoID={aid} expediente={s.get('numExpediente')} anio={s.get('anio')}")
    print(f"  archivoURL={arch}")
    found = None
    for tpl in HOST_CANDS:
        url = tpl.format(a=arch, ie=ie, aid=aid)
        try:
            r = c.head(url, timeout=10)
            if r.status_code < 400:
                size = r.headers.get("content-length", "?")
                ctype = r.headers.get("content-type", "?")
                print(f"  HEAD {r.status_code} size={size} ctype={ctype}  <- {url}")
                if not found:
                    found = url
        except Exception:
            pass
    results.append({"idEngrose": ie, "asuntoID": aid, "archivoURL": arch, "found_url": found, "meta": s})

# 3. If we found a host, download the 10 piloto.
hits = [r for r in results if r["found_url"]]
print(f"\nHits: {len(hits)}/{len(samples)}")
total_bytes = 0
for h in hits:
    url = h["found_url"]
    out = OUT_DIR / f"{h['idEngrose']}_{Path(h['archivoURL']).name}"
    try:
        r = c.get(url, timeout=120)
        if r.status_code == 200:
            out.write_bytes(r.content)
            total_bytes += len(r.content)
            print(f"  saved {out.name}: {len(r.content):,} bytes")
            time.sleep(0.3)
    except Exception as e:
        print(f"  fail {url}: {e}")

(OUT_DIR / "pilot_meta.json").write_text(json.dumps(results, ensure_ascii=False, indent=2))

print(f"\nTotal pilot bytes: {total_bytes:,}")
if hits:
    avg = total_bytes / len(hits)
    print(f"Avg per sentencia: {avg:,.0f} bytes")
    forecast = avg * 104525
    print(f"Forecast for 104,525 sentencias: {forecast/1e9:.1f} GB")
