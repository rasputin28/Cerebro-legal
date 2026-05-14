"""
Inspect /documento/tesis/<registroDigital> full payload across épocas.
Goal: confirm urlSemanario + precedentes + texto + materias + votación are
present, measure avg size, decide final schema columns.
"""
import json, httpx
from collections import Counter

BJ = "https://bj.scjn.gob.mx/api/v1/bj"
H = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-MX,es;q=0.9",
    "Origin": "https://bj.scjn.gob.mx",
    "Referer": "https://bj.scjn.gob.mx/",
    "Content-Type": "application/json",
}

c = httpx.Client(http2=True, timeout=60, headers=H)

# Pull 3 docs per época to compare shapes
samples = []
for ep_num in ["5", "8", "10", "11"]:
    b = {"q":"*","page":1,"size":3,"indice":"tesis","fuente":None,
         "extractos":200,"semantica":0,
         "filtros":{"epoca.numero":[ep_num]},
         "sortField":"","sortDireccion":""}
    r = c.post(f"{BJ}/busqueda", json=b)
    if r.status_code == 200:
        for d in r.json().get("resultados", []):
            samples.append((ep_num, d.get("registroDigital")))

print(f"sampling {len(samples)} tesis details")

all_keys = Counter()
sizes = []
for ep, rd in samples:
    if not rd:
        continue
    r = c.get(f"{BJ}/documento/tesis/{rd}")
    if r.status_code != 200:
        print(f"  [{ep}] {rd}: FAIL {r.status_code}")
        continue
    j = r.json()
    sizes.append(len(r.content))
    for k in j.keys():
        all_keys[k] += 1
    print(f"\n=== Época {ep} — registroDigital={rd} ({len(r.content):,} bytes) ===")
    # Print non-trivial fields
    for k, v in j.items():
        if v in (None, "", [], {}, 0):
            continue
        if isinstance(v, (dict, list)):
            s = json.dumps(v, ensure_ascii=False)
            if len(s) > 250:
                print(f"  {k} [{type(v).__name__} len={len(v) if hasattr(v,'__len__') else '?'}]: {s[:250]}...")
            else:
                print(f"  {k}: {s}")
        elif isinstance(v, str) and len(v) > 250:
            print(f"  {k} [str len={len(v)}]: {v[:250]}...")
        else:
            print(f"  {k}: {v}")

print(f"\n\n=== ALL KEYS observed across {len(samples)} samples ===")
for k, n in all_keys.most_common():
    print(f"  {n:>3}/{len(samples)}  {k}")

if sizes:
    print(f"\nAvg size: {sum(sizes)/len(sizes):,.0f} bytes")
    print(f"Min/Max: {min(sizes):,} / {max(sizes):,}")
    print(f"Forecast for 311,364: {sum(sizes)/len(sizes) * 311364 / 1e9:.2f} GB")
