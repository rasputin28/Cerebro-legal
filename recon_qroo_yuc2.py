"""Deep recon for QRoo + Yucatán legislation."""
import httpx, re
H = {"User-Agent": "Mozilla/5.0 Chrome/131.0"}

# Try common URL paths
PATHS = [
    "https://congresoqroo.gob.mx/marco-juridico/",
    "https://www.congresoqroo.gob.mx/marco-juridico/",
    "https://www.congresoqroo.gob.mx/leyes/",
    "https://www.congresoqroo.gob.mx/legislacion/",
    "https://congresoyucatan.gob.mx/marco-juridico/",
    "https://congresoyucatan.gob.mx/legislacion/",
    "https://congresoyucatan.gob.mx/leyes-y-decretos/",
    "https://congresoyucatan.gob.mx/legislacion/leyes",
    "https://www.congresoyucatan.gob.mx/legislacion",
]
for u in PATHS:
    try:
        r = httpx.get(u, headers=H, follow_redirects=True, verify=False, timeout=15)
        print(f"{r.status_code} {len(r.text):>7} {u}")
    except Exception as e:
        print(f"ERR {u}: {e}")

# Look for legislation-pointing links inside each congress homepage
print("\n=== QRoo links containing 'ley'/'legisl' ===")
r = httpx.get("https://www.congresoqroo.gob.mx/", headers=H, follow_redirects=True, verify=False, timeout=15)
links = re.findall(r'href="([^"]+)"', r.text)
matches = sorted({l for l in links if re.search(r'(legisl|ley|marco|codig|reglamen)', l, re.I)})
for l in matches[:25]:
    print(f"  {l[:150]}")

print("\n=== Yuc links containing 'ley'/'legisl' ===")
r = httpx.get("https://congresoyucatan.gob.mx/", headers=H, follow_redirects=True, verify=False, timeout=15)
links = re.findall(r'href="([^"]+)"', r.text)
matches = sorted({l for l in links if re.search(r'(legisl|ley|marco|codig|reglamen)', l, re.I)})
for l in matches[:25]:
    print(f"  {l[:150]}")
