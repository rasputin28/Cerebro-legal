"""Quick final checks: size=100 paginate behavior, texto dict structure, rate-limit feel."""
import json, time, httpx

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

# 1. size=100
r = c.post(f"{BJ}/busqueda", json={"q":"*","page":1,"size":100,"indice":"tesis","fuente":None,"extractos":200,"semantica":0,"filtros":{"epoca.numero":["11"]},"sortField":"","sortDireccion":""})
print(f"size=100 epoca=11: status={r.status_code} got={len(r.json().get('resultados',[]))}")

# 2. size=200 (push)
r = c.post(f"{BJ}/busqueda", json={"q":"*","page":1,"size":200,"indice":"tesis","fuente":None,"extractos":200,"semantica":0,"filtros":{"epoca.numero":["11"]},"sortField":"","sortDireccion":""})
print(f"size=200 epoca=11: status={r.status_code} got={len(r.json().get('resultados',[]))}")

# 3. texto dict structure
print("\n=== texto sub-keys across 5 docs ===")
for rd in [2023906, 2023907, 2023905, 2000469, 2000449]:
    r = c.get(f"{BJ}/documento/tesis/{rd}")
    if r.status_code == 200:
        j = r.json()
        texto = j.get("texto", {})
        if isinstance(texto, dict):
            print(f"  {rd}: texto keys = {list(texto.keys())}")
            for k, v in texto.items():
                preview = str(v)[:90].replace("\n"," ")
                print(f"     {k}: {preview!r}")

# 4. Rate-limit feel: 30 sequential requests at 0.1s
print("\n=== rate test: 30 requests at 0.1s sleep ===")
start = time.time()
ok = 0
for i in range(30):
    r = c.post(f"{BJ}/busqueda", json={"q":"*","page":1,"size":1,"indice":"tesis","fuente":None,"extractos":50,"semantica":0,"filtros":{"epoca.numero":["11"]},"sortField":"","sortDireccion":""})
    if r.status_code == 200:
        ok += 1
    else:
        print(f"  req {i}: {r.status_code}")
    time.sleep(0.1)
print(f"  {ok}/30 OK in {time.time()-start:.1f}s")
