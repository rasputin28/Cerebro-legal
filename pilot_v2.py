"""
Pilot download v2 — using the real storage endpoint found via Playwright recon.
GET /api/v1/bj/storage/sentencia?externo=true&fileparams=filename:<basename>
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

OUT_DIR = Path(__file__).parent / "sentencias_pilot"
OUT_DIR.mkdir(exist_ok=True)

# Wipe old garbage HTML files
for f in OUT_DIR.glob("*.doc*"):
    f.unlink()


def basename_no_ext(archivo_url: str) -> str:
    base = archivo_url.split("/")[-1]
    return base.rsplit(".", 1)[0] if "." in base else base


with httpx.Client(http2=True, timeout=120, headers=H, follow_redirects=True) as c:
    # Pull a diverse pilot set across years
    samples = []
    for year in [2024, 2020, 2015, 2010, 2005, 2000, 1995]:
        body = {
            "q": "*", "page": 1, "size": 2,
            "indice": "sentencias_pub", "fuente": None,
            "extractos": 200, "semantica": 0,
            "filtros": {"anio": [str(year)]},
            "sortField": "", "sortDireccion": "",
        }
        r = c.post(f"{BJ}/busqueda", json=body)
        if r.status_code == 200:
            docs = r.json().get("resultados", [])
            print(f"year={year}: {len(docs)} docs, total in year={r.json().get('total')}")
            samples.extend(docs)

    # Dedup
    seen = set()
    uniq = []
    for d in samples:
        ie = d.get("idEngrose")
        if ie and ie not in seen:
            seen.add(ie)
            uniq.append(d)
    samples = uniq[:10]
    print(f"\nUnique pilot: {len(samples)} sentencias\n")

    results = []
    total_bytes = 0
    by_year = {}
    for s in samples:
        arch = s.get("archivoURL", "")
        ie = s.get("idEngrose")
        anio = s.get("anio")
        bn = basename_no_ext(arch)
        url = f"{BJ}/storage/sentencia?externo=true&fileparams=filename:{bn}"
        try:
            r = c.get(url)
            ct = r.headers.get("content-type", "")
            disp = r.headers.get("content-disposition", "")
            print(f"idEngrose={ie} anio={anio} bn={bn}")
            print(f"  status={r.status_code} ctype={ct} size={len(r.content):,} disposition={disp}")
            if r.status_code == 200 and len(r.content) > 1000 and ("pdf" in ct or "officedocument" in ct or "msword" in ct):
                ext = "pdf" if "pdf" in ct else "docx" if "openxml" in ct else "doc"
                out = OUT_DIR / f"{ie}_{anio}_{bn}.{ext}"
                out.write_bytes(r.content)
                total_bytes += len(r.content)
                by_year.setdefault(anio, []).append(len(r.content))
                results.append({"idEngrose": ie, "anio": anio, "url": url, "bytes": len(r.content), "ctype": ct, "saved": str(out.name)})
                print(f"  saved {out.name}")
            else:
                results.append({"idEngrose": ie, "anio": anio, "url": url, "bytes": len(r.content), "ctype": ct, "saved": None})
        except Exception as e:
            print(f"  ERR: {e}")
        time.sleep(0.5)

(OUT_DIR / "pilot_v2_meta.json").write_text(json.dumps(results, ensure_ascii=False, indent=2))

print("\n=== summary ===")
print(f"Total bytes: {total_bytes:,}")
hits = [r for r in results if r.get("saved")]
if hits:
    avg = total_bytes / len(hits)
    print(f"Avg per sentencia: {avg:,.0f} bytes")
    print(f"Forecast for 104,525 sentencias: {(avg * 104525) / 1e9:.1f} GB")
print(f"By year:")
for y, sizes in sorted(by_year.items(), key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0):
    print(f"  {y}: {len(sizes)} files, avg {sum(sizes)/len(sizes):,.0f} bytes")
