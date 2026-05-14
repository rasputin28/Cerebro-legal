import sqlite3, re
c = sqlite3.connect("sre.db")
html = c.execute("SELECT detail_html FROM sre_tratados LIMIT 1").fetchone()[0]
text = re.sub(r"<script.*?</script>", "", html, flags=re.S|re.I)
text = re.sub(r"<style.*?</style>", "", text, flags=re.S|re.I)

# look at section before first <b>Categoría:
first_b = text.lower().find("<b>categor")
if first_b > 0:
    snippet = text[max(0, first_b-1500):first_b]
    print("=== raw HTML 1500 chars BEFORE <b>Categoría: ===")
    print(snippet[-1500:])
    print("\n=== cleaned text ===")
    cleaned = re.sub(r"<[^>]+>", " ", snippet)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    print(cleaned[-800:])
