"""Audit which substantive codes exist in bj.scjn for the top 12 priority states."""
import sqlite3
import re

DB = "legislacion.db"
conn = sqlite3.connect(DB)

STATES_PRIORITY = [
    "ESTADO DE MEXICO", "JALISCO", "NUEVO LEON", "PUEBLA", "VERACRUZ",
    "CHIAPAS", "OAXACA", "GUERRERO", "SINALOA", "CHIHUAHUA",
    "BAJA CALIFORNIA", "MICHOACAN", "QUINTANA ROO", "YUCATAN",
    "CIUDAD DE MEXICO", "GUANAJUATO",
]

# Códigos sustantivos + procesales + orgánica que necesitamos por estado
CODIGOS_NEEDED = [
    ("Constitución",       r"CONSTITUCI[OÓ]N\s+POL[IÍ]TICA\s+DEL\s+ESTADO"),
    ("Código Civil",       r"^C[OÓ]DIGO\s+CIVIL\s+(?:PARA\s+EL|DEL)\s+ESTADO"),
    ("Código Penal",       r"^C[OÓ]DIGO\s+PENAL\s+(?:PARA\s+EL|DEL)\s+ESTADO"),
    ("Código Fiscal",      r"^C[OÓ]DIGO\s+FISCAL\s+(?:PARA\s+EL|DEL)\s+ESTADO"),
    ("CPC (Proc Civil)",   r"^C[OÓ]DIGO\s+(?:DE\s+)?PROCEDIMIENTOS\s+CIVILES"),
    ("CPP (Proc Penal)",   r"^C[OÓ]DIGO\s+(?:DE\s+)?PROCEDIMIENTOS\s+PENALES|^C[OÓ]DIGO\s+PROCESAL\s+PENAL"),
    ("Proc Familiar",      r"^C[OÓ]DIGO\s+(?:DE\s+)?PROCEDIMIENTOS\s+FAMILIARES|^C[OÓ]DIGO\s+(?:DE\s+|DE\s+LA\s+)?FAMILIA"),
    ("Ley Orgánica PJ",    r"^LEY\s+ORG[AÁ]NICA\s+(?:DEL\s+)?PODER\s+JUDICIAL"),
]

# Pre-build all (estado, ordenamiento) tuples from DB once
print("Loading state legislation...")
rows = conn.execute("""
    SELECT estado, ordenamiento FROM legislacion
    WHERE ambito='ESTATAL' AND vigencia='VIGENTE'
""").fetchall()
print(f"  {len(rows)} estatal vigente rows\n")

# Per state, check each code
print(f"{'Estado':<22} | " + " | ".join(c[0][:10] for c in CODIGOS_NEEDED))
print("-" * 130)
gap_count = 0
results = {}
for state in STATES_PRIORITY:
    state_rows = [o for s, o in rows if s and s.upper() == state]
    presence = []
    for label, pat in CODIGOS_NEEDED:
        regex = re.compile(pat, re.I)
        matches = [o for o in state_rows if regex.search(o or "")]
        presence.append("✅" if matches else "❌")
        if not matches: gap_count += 1
    results[state] = presence
    print(f"{state:<22} | " + " | ".join(f"{p:<10}" for p in presence))

print("\n--- Sample to verify a hit (Jalisco Código Civil) ---")
for r in conn.execute("""
    SELECT ordenamiento, fechaPublicado, vigencia FROM legislacion
    WHERE ambito='ESTATAL' AND estado='JALISCO' AND ordenamiento LIKE 'CODIGO CIVIL%' LIMIT 3
"""):
    print(f"  {r[0][:80]} | {r[1]} | {r[2]}")

print(f"\nTOTAL gaps across {len(STATES_PRIORITY)} priority states × {len(CODIGOS_NEEDED)} codes: {gap_count} / {len(STATES_PRIORITY)*len(CODIGOS_NEEDED)}")
