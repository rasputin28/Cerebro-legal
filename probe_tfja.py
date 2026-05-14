"""
TFJA probe — fill the three open gaps from the user's brief:
  1. ID mínimo válido
  2. ¿Headers requeridos?
  3. ¿Hay índice alternativo / paginación?
"""
import httpx, time

BASE = "https://www.tfja.gob.mx/cesmdfa/sctj/tesis-pdf-detalle/{}/"
INDEX_CANDIDATES = [
    "https://www.tfja.gob.mx/cesmdfa/sctj/",
    "https://www.tfja.gob.mx/cesmdfa/sctj/tesis-pdf-detalle/",
    "https://www.tfja.gob.mx/cesmdfa/",
    "https://www.tfja.gob.mx/cesmdfa/sctj/listado",
    "https://www.tfja.gob.mx/cesmdfa/sctj/criterios",
    "https://www.tfja.gob.mx/cesmdfa/sctj/buscador",
]

H_NONE = {}
H_MIN = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36"}
H_FULL = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
    "Accept": "application/pdf,*/*",
    "Accept-Language": "es-MX,es;q=0.9",
    "Referer": "https://www.tfja.gob.mx/cesmdfa/sctj/",
}


def hit(client, url, headers, label):
    try:
        r = client.get(url, headers=headers, timeout=20)
        ct = r.headers.get("content-type", "")
        size = len(r.content)
        magic = r.content[:8].hex()
        pdf_ok = r.content[:5] == b"%PDF-"
        print(f"  [{label:8s}] {r.status_code:>3}  size={size:>9}  ctype={ct[:30]:<30}  magic={magic}  is_pdf={pdf_ok}")
        return r
    except Exception as e:
        print(f"  [{label:8s}] ERR: {e}")
        return None


with httpx.Client(http2=True, follow_redirects=True, timeout=30) as c:
    print("=" * 80)
    print("STEP 1: Confirm the 4 known good IDs return PDF + header sensitivity")
    print("=" * 80)
    for tid in [44948, 45775, 46285, 46500]:
        url = BASE.format(tid)
        print(f"\nID {tid} → {url}")
        for label, h in [("no-hdr", H_NONE), ("min-hdr", H_MIN), ("full-hdr", H_FULL)]:
            hit(c, url, h, label)
            time.sleep(0.3)

    print("\n" + "=" * 80)
    print("STEP 2: Find lower / upper bound of ID space")
    print("=" * 80)
    for tid in [1, 10, 50, 100, 500, 1000, 5000, 10000, 20000, 30000, 40000, 47000, 47500, 48000, 50000, 60000]:
        url = BASE.format(tid)
        r = hit(c, url, H_MIN, f"id={tid}")
        time.sleep(0.2)

    print("\n" + "=" * 80)
    print("STEP 3: Look for alternate index / paginated listing")
    print("=" * 80)
    for url in INDEX_CANDIDATES:
        try:
            r = c.get(url, headers=H_MIN, timeout=15)
            ct = r.headers.get("content-type", "")
            n = len(r.content)
            print(f"  {r.status_code} ctype={ct[:30]:<30} size={n:>7}  <- {url}")
            # If HTML, scan briefly for telling links
            if "html" in ct and r.status_code == 200:
                text = r.text
                # quick links scan
                import re
                hits = re.findall(r'tesis-pdf-detalle/(\d+)/?', text)
                if hits:
                    print(f"     found {len(hits)} tesis IDs on page; sample: {hits[:5]}")
                paths = re.findall(r'href=["\\\']([^"\\\']+)["\\\']', text)
                interesting = [p for p in paths if "cesmdfa" in p or "sctj" in p][:10]
                if interesting:
                    print(f"     internal links sample: {interesting}")
        except Exception as e:
            print(f"  ERR {url}: {e}")
        time.sleep(0.3)

    print("\n" + "=" * 80)
    print("STEP 4: JUR_SUSP_MOD.pdf sanity check")
    print("=" * 80)
    r = c.get("https://www.tfja.gob.mx/media/media/pdf/cesmdfa/scjl/JUR_SUSP_MOD.pdf", headers=H_MIN, timeout=30)
    print(f"  status={r.status_code} size={len(r.content):,} ctype={r.headers.get('content-type','')}")
