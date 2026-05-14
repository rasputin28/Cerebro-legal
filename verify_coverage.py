"""Verify each claim in the coverage map with data, not inference."""
import sqlite3, json
import httpx

tesis = sqlite3.connect("tesis.db")
tesis.row_factory = sqlite3.Row
hist  = sqlite3.connect("sjf.db")
hist.row_factory = sqlite3.Row

print("="*80)
print("CLAIM 1: épocas 3-4 in tesis.db are TEPJF (not SCJN)")
print("="*80)
# Show fuente, órgano, sala examples per época
for ep in ["3","4"]:
    print(f"\n--- época {ep}: fuente distribution ---")
    rows = tesis.execute("SELECT fuente, COUNT(*) AS n FROM tesis WHERE epoca_numero=? GROUP BY fuente", (ep,)).fetchall()
    for r in rows:
        print(f"  {r['n']:>5}  {r['fuente']}")
    print(f"--- época {ep}: órgano distribution ---")
    rows = tesis.execute("SELECT organo_jurisdiccional, COUNT(*) AS n FROM tesis WHERE epoca_numero=? GROUP BY organo_jurisdiccional", (ep,)).fetchall()
    for r in rows:
        print(f"  {r['n']:>5}  {r['organo_jurisdiccional']}")
    print(f"--- época {ep}: instancia distribution ---")
    rows = tesis.execute("SELECT instancia, COUNT(*) AS n FROM tesis WHERE epoca_numero=? GROUP BY instancia", (ep,)).fetchall()
    for r in rows:
        print(f"  {r['n']:>5}  {r['instancia']}")
    print(f"--- época {ep}: año range ---")
    r = tesis.execute("SELECT MIN(anio) min_y, MAX(anio) max_y FROM tesis WHERE epoca_numero=?", (ep,)).fetchone()
    print(f"  min={r['min_y']}  max={r['max_y']}")

print()
print("="*80)
print("CLAIM 2: Sala Superior (1,017) = TEPJF entirely?")
print("="*80)
# Show épocas + claves + fuente of Sala Superior rows
rows = tesis.execute("SELECT epoca_numero, fuente, COUNT(*) AS n FROM tesis WHERE instancia='Sala Superior' GROUP BY epoca_numero, fuente ORDER BY n DESC").fetchall()
print(f"\nBy época + fuente:")
for r in rows:
    print(f"  ep={r['epoca_numero']:<3}  {r['n']:>5}  {r['fuente']}")
print("\nFirst 5 claves (TEPJF uses S.S./J.X/YYYY; SCJN uses 1a./J. X/YYYY style):")
rows = tesis.execute("SELECT clave, rubro FROM tesis WHERE instancia='Sala Superior' AND clave IS NOT NULL LIMIT 5").fetchall()
for r in rows:
    print(f"  {r['clave']}  ::  {r['rubro'][:80] if r['rubro'] else ''}...")
print("\nSample raw fuente strings:")
rows = tesis.execute("SELECT DISTINCT fuente FROM tesis WHERE instancia='Sala Superior' LIMIT 10").fetchall()
for r in rows:
    print(f"  {r['fuente']}")

print()
print("="*80)
print("CLAIM 3: Sala Regional Monterrey only 2 — incompleto vs. correct?")
print("="*80)
rows = tesis.execute("SELECT registro_digital, clave, anio, rubro FROM tesis WHERE instancia='Sala Regional Monterrey'").fetchall()
for r in rows:
    print(f"  rd={r['registro_digital']}  clave={r['clave']}  año={r['anio']}")
    print(f"     {r['rubro'][:120] if r['rubro'] else ''}")

# Check if other Salas Regionales (Xalapa, Guadalajara, CDMX/DF, Toluca, Especializada) appear under any instancia label
print("\nSearch for any other regional sala via órgano:")
rows = tesis.execute("""
SELECT organo_jurisdiccional, COUNT(*) AS n FROM tesis
WHERE organo_jurisdiccional LIKE '%Regional%' OR organo_jurisdiccional LIKE '%Guadalajara%'
   OR organo_jurisdiccional LIKE '%Xalapa%' OR organo_jurisdiccional LIKE '%Toluca%'
   OR organo_jurisdiccional LIKE '%Especializada%' OR organo_jurisdiccional LIKE '%Distrito Federal%'
GROUP BY organo_jurisdiccional ORDER BY n DESC LIMIT 20
""").fetchall()
for r in rows:
    print(f"  {r['n']:>5}  {r['organo_jurisdiccional']}")

print()
print("="*80)
print("CLAIM 4: TFJA absent — verify by searching common TFJA strings")
print("="*80)
patterns = ["%TFJA%", "%Fiscal Adm%", "%Tribunal Federal%", "%Sala Regional Hacendaria%", "%TFF%"]
for p in patterns:
    n = tesis.execute(f"SELECT COUNT(*) FROM tesis WHERE instancia LIKE ? OR organo_jurisdiccional LIKE ? OR fuente LIKE ?", (p,p,p)).fetchone()[0]
    print(f"  {p}: {n} hits")

# Also try in fuente
print("\nAll distinct fuente values containing 'tribunal' (case-insensitive):")
rows = tesis.execute("SELECT DISTINCT fuente FROM tesis WHERE LOWER(fuente) LIKE '%tribunal%'").fetchall()
for r in rows:
    print(f"  {r['fuente']}")

print()
print("="*80)
print("CLAIM 5: tesis.db vs sjf.db — any overlap?")
print("="*80)
# Sample 20 rubros from sjf.db épocas 3-4 and search in tesis.db
print("\nsjf.db has these 4 épocas:")
rows = hist.execute("SELECT epoca, COUNT(*) AS n FROM tesis GROUP BY epoca").fetchall()
for r in rows:
    print(f"  {r['n']:>5}  {r['epoca']}")
print("\nsjf.db Tercera+Cuarta Época: 4 sample rubros, look up in tesis.db:")
rows = hist.execute("SELECT ius, rubro FROM tesis WHERE epoca IN ('Tercera Época','Cuarta Época') ORDER BY RANDOM() LIMIT 4").fetchall()
for r in rows:
    head = (r['rubro'] or '')[:60].replace("\n"," ")
    hit = tesis.execute("SELECT COUNT(*) FROM tesis WHERE rubro LIKE ? LIMIT 1", (head + "%",)).fetchone()[0]
    print(f"  sjf ius={r['ius']}: '{head}…'   match in tesis.db: {hit}")

# Inverse: tesis.db epoca 5 (oldest modern) — any rubros found in sjf.db?
print("\ntesis.db época 5: 3 sample rubros, look up in sjf.db:")
rows = tesis.execute("SELECT registro_digital, rubro FROM tesis WHERE epoca_numero='5' ORDER BY RANDOM() LIMIT 3").fetchall()
for r in rows:
    head = (r['rubro'] or '')[:60].replace("\n"," ")
    hit = hist.execute("SELECT COUNT(*) FROM tesis WHERE rubro LIKE ?", (head + "%",)).fetchone()[0]
    print(f"  rd={r['registro_digital']}: '{head}…'   match in sjf.db: {hit}")

print()
print("="*80)
print("CLAIM 6: 'todas las épocas SCJN' — what's the SCJN-by-época breakdown?")
print("="*80)
rows = tesis.execute("""
SELECT epoca_numero, epoca_nombre, COUNT(*) AS n,
       MIN(anio) AS y_min, MAX(anio) AS y_max
FROM tesis WHERE instancia='Suprema Corte de Justicia de la Nación'
GROUP BY epoca_numero ORDER BY CAST(epoca_numero AS INTEGER)
""").fetchall()
for r in rows:
    print(f"  ép {r['epoca_numero']}  ({r['epoca_nombre']})  n={r['n']:<7}  años={r['y_min']}-{r['y_max']}")

print()
print("Historica DB by epoca:")
rows = hist.execute("SELECT epoca, COUNT(*) AS n FROM tesis GROUP BY epoca ORDER BY epoca").fetchall()
for r in rows:
    print(f"  {r['epoca']}: {r['n']}")

print()
print("="*80)
print("Number of TEPJF Salas Regionales (TEPJF has 6 since 2017, was 5 before)")
print("="*80)
print("Salas Regionales TEPJF historicamente: Guadalajara, Monterrey, Xalapa, DF/CDMX, Toluca, + Especializada (2017+)")
print("That is 6, not 5. Need to correct previous claim.")
