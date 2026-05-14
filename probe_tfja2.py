"""
Follow-up:
1. Inspect the 5KB placeholder PDF (so we know how to skip invalid IDs).
2. Probe the /cesmdfa/sctj/sctj-busqueda/ for an alt index or AJAX endpoint.
3. Find precise upper bound between 48000 and 50000.
"""
import httpx, time, re
import pdfplumber, io

H = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0"}

with httpx.Client(http2=True, follow_redirects=True, timeout=30, headers=H) as c:
    # 1. Save the placeholder PDF + a real one, extract text, compare.
    print("=" * 80)
    print("STEP 1: placeholder vs real PDF content")
    print("=" * 80)
    for tid in [50000, 60000, 44948]:
        url = f"https://www.tfja.gob.mx/cesmdfa/sctj/tesis-pdf-detalle/{tid}/"
        r = c.get(url)
        print(f"\nID {tid}: size={len(r.content):,}")
        try:
            with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                print(f"  pages: {len(pdf.pages)}")
                for i, p in enumerate(pdf.pages[:2]):
                    txt = (p.extract_text() or "")[:500]
                    print(f"  page {i+1} text[:500]:")
                    for line in txt.split("\n")[:10]:
                        print(f"    {line}")
        except Exception as e:
            print(f"  pdf parse err: {e}")

    # 2. Binary search the upper bound between 48000 and 50000.
    print("\n" + "=" * 80)
    print("STEP 2: upper bound between 48000 and 50000")
    print("=" * 80)
    lo, hi = 48000, 50000
    while lo < hi - 1:
        mid = (lo + hi) // 2
        r = c.get(f"https://www.tfja.gob.mx/cesmdfa/sctj/tesis-pdf-detalle/{mid}/")
        sz = len(r.content)
        is_real = sz > 20000
        print(f"  id={mid} size={sz:,} {'REAL' if is_real else 'placeholder'}")
        if is_real:
            lo = mid
        else:
            hi = mid
        time.sleep(0.2)
    print(f"  --> upper bound: max valid ID ≈ {lo}")

    # 3. Buscador alt index.
    print("\n" + "=" * 80)
    print("STEP 3: explore sctj-busqueda/")
    print("=" * 80)
    r = c.get("https://www.tfja.gob.mx/cesmdfa/sctj/sctj-busqueda/")
    print(f"status={r.status_code} size={len(r.content):,} ctype={r.headers.get('content-type','')}")
    text = r.text
    # forms, AJAX endpoints, paginated routes
    print("\nLook for form action:")
    for m in re.finditer(r'<form[^>]+action=["\\\']([^"\\\']+)["\\\']', text, re.I)[:5] if False else re.finditer(r'<form[^>]+action=["\\\']([^"\\\']+)["\\\']', text, re.I):
        print(f"  form action: {m.group(1)}")
    print("\nAJAX URLs in <script>:")
    for m in re.finditer(r'(?:url|action)\s*[:=]\s*["\\\']([^"\\\']+sctj[^"\\\']+)["\\\']', text, re.I):
        print(f"  {m.group(1)}")
    print("\nAll cesmdfa/sctj URLs in page:")
    seen = set()
    for m in re.finditer(r'["\\\'](/?[\w\-\./]*cesmdfa[\w\-\./]*sctj[\w\-\./]*)["\\\']', text):
        u = m.group(1)
        if u not in seen:
            seen.add(u)
            print(f"  {u}")
    print("\nFirst 1500 chars of body:")
    body_start = text.find("<body")
    print(text[body_start:body_start+1500] if body_start > 0 else text[:1500])
