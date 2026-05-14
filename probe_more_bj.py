"""Probe additional bj.scjn sources for total counts and key fields."""
import httpx
H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json",
     "Content-Type": "application/json", "Origin": "https://bj.scjn.gob.mx",
     "Referer": "https://bj.scjn.gob.mx/"}

INDICES = ["acuerdos", "legislacion", "vtaquigraficas", "expedientes_pub",
           "votos_sentencias_pub", "biblioteca", "ccj_cursos"]

for indice in INDICES:
    body = {"q": "*", "page": 1, "size": 1, "indice": indice, "fuente": None,
            "extractos": 50, "semantica": 0, "filtros": {},
            "sortField": "", "sortDireccion": ""}
    try:
        r = httpx.post("https://bj.scjn.gob.mx/api/v1/bj/busqueda",
                       headers=H, json=body, timeout=20)
        if r.status_code == 200:
            j = r.json()
            total = j.get("total")
            res = j.get("resultados", [])
            keys = list(res[0].keys())[:10] if res else []
            print(f"{indice:25s}  total={total:>6}  first keys: {keys}")
        else:
            print(f"{indice:25s}  status={r.status_code}")
    except Exception as e:
        print(f"{indice:25s}  ERR {e}")
