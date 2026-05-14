import httpx, re
r = httpx.get("https://www.diputados.gob.mx/LeyesBiblio/index.htm",
              headers={"User-Agent": "Mozilla/5.0 Chrome/131.0"}, follow_redirects=True, timeout=30)
html = r.text
# Find rows that point to law detail files
links = re.findall(r'<a[^>]+href="([^"]+\.(?:pdf|doc|docx|htm))"[^>]*>([^<]+)</a>', html, re.I)
print(f"links found: {len(links)}")
# Sample
for url, name in links[:20]:
    print(f"  {url[:90]:90}  {name[:40]!r}")

# Look at any structural patterns
print("\n--- looking for context around 'Constituci' ---")
i = html.lower().find("constituci")
if i > 0:
    print(html[max(0,i-200):i+500])
