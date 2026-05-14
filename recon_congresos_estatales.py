"""Probe all 32 state congresses for accessible legislation pages."""
import httpx, re

H = {"User-Agent": "Mozilla/5.0 Chrome/131.0"}

# Known patterns: <subdomain>.gob.mx, congreso<state>.gob.mx, etc.
CONGRESSES = {
    "AGS": ["https://www.congresoags.gob.mx/", "https://www.congresoaguascalientes.gob.mx/"],
    "BC":  ["https://www.congresobc.gob.mx/"],
    "BCS": ["https://www.cbcs.gob.mx/"],
    "CAM": ["https://www.congresocam.gob.mx/", "https://www.congresodecampeche.gob.mx/"],
    "CHIS":["https://www.congresochiapas.gob.mx/"],
    "CHIH":["https://www.congresochihuahua.gob.mx/"],
    "CDMX":["https://www.congresocdmx.gob.mx/"],
    "COAH":["https://www.congresocoahuila.gob.mx/"],
    "COL": ["https://www.congresocol.gob.mx/"],
    "DGO": ["https://www.congresodurango.gob.mx/"],
    "EDOMEX":["https://www.cddiputados.gob.mx/", "https://www.legislativo.edomex.gob.mx/"],
    "GTO": ["https://www.congresogto.gob.mx/"],
    "GRO": ["https://www.congresogro.gob.mx/"],
    "HGO": ["https://www.congreso-hidalgo.gob.mx/"],
    "JAL": ["https://www.congresojal.gob.mx/"],
    "MICH":["https://congresomich.gob.mx/"],
    "MOR": ["https://www.congresomorelos.gob.mx/"],
    "NAY": ["https://www.congresonayarit.mx/"],
    "NL":  ["https://www.hcnl.gob.mx/"],
    "OAX": ["https://www.congresooaxaca.gob.mx/"],
    "PUE": ["https://congresopuebla.gob.mx/"],
    "QRO": ["https://legislaturaqueretaro.gob.mx/"],
    "QROO":["https://www.congresoqroo.gob.mx/"],
    "SLP": ["https://www.congresoslp.gob.mx/"],
    "SIN": ["https://www.congresosinaloa.gob.mx/"],
    "SON": ["https://www.congresoson.gob.mx/"],
    "TAB": ["https://www.congresotabasco.gob.mx/"],
    "TAMS":["https://www.congresotamaulipas.gob.mx/"],
    "TLAX":["https://www.congresotlaxcala.gob.mx/"],
    "VER": ["https://www.legisver.gob.mx/"],
    "YUC": ["https://congresoyucatan.gob.mx/"],
    "ZAC": ["https://www.congresozac.gob.mx/"],
}

# Common paths legislation lives at
PATHS = ["leyes/", "marco-juridico/", "legislacion/", "legislacion/leyes",
         "marco_juridico/", "leyes-y-decretos/", "legislacion-vigente/",
         "Legislacion/", "BibliotecaLegislativa/"]


def probe(home: str):
    """Return list of (path, status, len) for paths likely to contain legislation."""
    out = []
    for p in PATHS:
        u = home.rstrip("/") + "/" + p
        try:
            r = httpx.get(u, headers=H, follow_redirects=True, timeout=12, verify=False)
            if r.status_code == 200 and len(r.text) > 5000:
                # Count PDF links
                pdfs = len(set(re.findall(r'href=["\']([^"\']+\.pdf)["\']', r.text)))
                if pdfs > 5:
                    out.append((u, r.status_code, len(r.text), pdfs))
        except Exception:
            pass
    return out


def main():
    print(f"Probing {len(CONGRESSES)} state congresses...")
    crawlable = {}
    for state, urls in CONGRESSES.items():
        found = []
        for home in urls:
            try:
                r = httpx.get(home, headers=H, follow_redirects=True, timeout=10, verify=False)
                if r.status_code != 200: continue
                hits = probe(home)
                if hits:
                    found.extend([(home,) + h for h in hits])
            except Exception:
                continue
        if found:
            crawlable[state] = found
            print(f"\n{state}: {len(found)} crawlable paths")
            for home, url, status, size, pdfs in found[:3]:
                print(f"  {url}  ({pdfs} PDFs in {size:,} bytes)")
        else:
            print(f"{state}: no crawlable legislation pages found")
    print(f"\n\nTotal crawlable states: {len(crawlable)}")
    print("States:", sorted(crawlable.keys()))


if __name__ == "__main__":
    main()
