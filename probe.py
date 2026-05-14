"""Probe the historicalfile endpoint directly with httpx and pull total count + sample."""
import json, httpx

URL = "https://sjf2.scjn.gob.mx/services/sjftesismicroservice/api/public/historicalfile"
BODY = {
    "classifiers": [
        {"name": "idEpoca", "value": ["4", "3", "2", "1"], "allSelected": False, "visible": False, "isMatrix": True},
        {"name": "idInstancia", "value": ["0", "1", "2", "3", "7"], "allSelected": False, "visible": False, "isMatrix": True},
    ],
    "searchTerms": [],
    "bFacet": True,
    "ius": [],
    "idApp": "SJFAPP2020",
    "lbSearch": ["Todo"],
    "filterExpression": "",
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Origin": "https://sjf2.scjn.gob.mx",
    "Referer": "https://sjf2.scjn.gob.mx/listado-tesis-historicas",
    "Content-Type": "application/json",
}

with httpx.Client(http2=True, timeout=30, headers=HEADERS) as c:
    r = c.post(URL, params={"page": 0, "size": 1}, json=BODY)
    print("STATUS:", r.status_code)
    if r.status_code == 200:
        j = r.json()
        print("TOP KEYS:", list(j.keys()))
        for k in j.keys():
            v = j[k]
            if k == "documents":
                print(f"  documents: list of {len(v)}; first keys:", list(v[0].keys()) if v else "[]")
            else:
                print(f"  {k}: {repr(v)[:200]}")
    else:
        print(r.text[:1000])
