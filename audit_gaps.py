"""Audit corpus gaps per CLAUDE.md analysis."""
import sqlite3

print("=== 1. CNPCyF en Diputados (317 leyes federales) ===")
c = sqlite3.connect("dip_leyes.db")
res = c.execute("""
    SELECT nombre FROM dip_leyes
    WHERE LOWER(nombre) LIKE '%procedimientos civiles%familiares%'
       OR LOWER(nombre) LIKE '%nacional de procedimientos civiles%'
       OR LOWER(nombre) LIKE '%cnpcyf%'
""").fetchall()
if res:
    for (n,) in res: print(f"  ✓ {n}")
else:
    print("  ❌ CNPCyF NO encontrado")
print("  Búsqueda más amplia 'procedimientos civiles':")
for (n,) in c.execute("SELECT nombre FROM dip_leyes WHERE LOWER(nombre) LIKE '%procedimientos civiles%'").fetchall()[:5]:
    print(f"  → {n}")

print("\n=== 2. Corte IDH Series breakdown ===")
c = sqlite3.connect("corteidh.db")
for r in c.execute("SELECT serie, tipo, COUNT(*) FROM idh_docs WHERE status='ok' GROUP BY serie, tipo ORDER BY 1"):
    print(f"  Serie {r[0]} | tipo={r[1]}: {r[2]}")

print("\n=== 3. bj.scjn legislación composición ===")
c = sqlite3.connect("legislacion.db")
print("Por ámbito:")
for r in c.execute("SELECT ambito, COUNT(*) FROM legislacion GROUP BY ambito ORDER BY 2 DESC"):
    a = r[0] or "(null)"
    print(f"  {a}: {r[1]}")

print("\nTipo de ordenamiento (primera palabra):")
tipo_counts = {}
for (s,) in c.execute("SELECT ordenamiento FROM legislacion WHERE ordenamiento IS NOT NULL"):
    first_word = s.split(" ")[0] if s else ""
    if first_word:
        tipo_counts[first_word] = tipo_counts.get(first_word, 0) + 1
for k, n in sorted(tipo_counts.items(), key=lambda x: -x[1])[:15]:
    print(f"  {k}: {n:,}")

print("\n=== 4. Días inhábiles en SCJN acuerdos (3,520) ===")
c = sqlite3.connect("acuerdos.db")
patterns = ["%inh%bil%", "%per%odo%vacacional%", "%suspensi%n de labores%", "%suspensi%n de t%rminos%", "%calendario%"]
total = 0
for p in patterns:
    n = c.execute(f"SELECT COUNT(*) FROM acuerdos WHERE LOWER(rubro) LIKE '{p}'").fetchone()[0]
    print(f"  pattern {p!r}: {n}")
    total += n
print("Muestra hits:")
for r in c.execute("""
    SELECT rubro FROM acuerdos
    WHERE LOWER(rubro) LIKE '%inh%bil%' OR LOWER(rubro) LIKE '%per%odo%vacacional%'
       OR LOWER(rubro) LIKE '%calendario%'
    LIMIT 6
"""):
    print(f"  → {(r[0] or '')[:120]}")

print("\n=== 5. NOMs en bj.scjn legislación ===")
c = sqlite3.connect("legislacion.db")
noms = c.execute("""
    SELECT COUNT(*) FROM legislacion
    WHERE ordenamiento LIKE 'NORMA OFICIAL%' OR ordenamiento LIKE 'NOM-%' OR ordenamiento LIKE '%NOM %'
""").fetchone()[0]
print(f"NOMs detectadas: {noms}")

print("\n=== 6. Constituciones estatales en bj.scjn ===")
const = c.execute("""
    SELECT estado, COUNT(*) FROM legislacion
    WHERE LOWER(ordenamiento) LIKE '%constituci%pol%tica%estado%' AND ambito='ESTATAL'
    GROUP BY estado ORDER BY 2 DESC
""").fetchall()
print(f"Estados con 'Constitución Política del Estado': {len(const)}")
for s, n in const[:20]:
    print(f"  {s}: {n}")

print("\n=== 7. Reglamentos federales en bj.scjn ===")
regfed = c.execute("""
    SELECT COUNT(*) FROM legislacion
    WHERE ordenamiento LIKE 'REGLAMENTO%' AND ambito='FEDERAL'
""").fetchone()[0]
print(f"Reglamentos federales: {regfed}")

print("\n=== 8. Votos sentencias con/sin texto ===")
c = sqlite3.connect("votos_sent.db")
for r in c.execute("""
    SELECT SUM(CASE WHEN textoVoto IS NOT NULL AND length(textoVoto) > 50 THEN 1 ELSE 0 END) AS con_texto,
           SUM(CASE WHEN textoVoto IS NULL OR length(textoVoto) < 50 THEN 1 ELSE 0 END) AS sin_texto
    FROM votos_sent
"""):
    print(f"  Con texto: {r[0]}, sin texto: {r[1]}")

print("\n=== 9. Yucatán leyes sin título — actual count ===")
c = sqlite3.connect("legis_estatales.db")
n = c.execute("SELECT COUNT(*) FROM legis WHERE estado='YUC' AND (nombre IS NULL OR nombre='' OR length(nombre) < 5)").fetchone()[0]
print(f"  Sin título reconocible: {n}")
