"""Re-parse sre_tratados detail_html with a simpler regex-based extractor."""
import re
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "sre.db"

LABEL_MAP = {
    "lugar y fecha de adopción": "lugar_adopcion",
    "categoría": "categoria",
    "estatus": "estatus",
    "tema": "tema",
    "notas": "notas",
    "trámite constitucional": "tramite_constitucional",
    "aprobación del senado": "aprobacion_senado",
    "publicación dof aprobación": "publ_dof_aprobacion",
    "entrada en vigor": "fecha_vigor_mexico",
    "publicación dof promulgación": "publ_dof_promulgacion",
}

# Loose categorization
DDHH_HINTS = re.compile(
    r"derechos\s+humanos|civiles|sociales|culturales|niñ|mujer|indígena|tortura|"
    r"discriminación|refugiad|asilo|migrant|libertad|igualdad|genocidio|"
    r"esclavitud|trata\s+de\s+personas|desaparición\s+forzada|defensores",
    re.I,
)
COMMERCIAL_HINTS = re.compile(
    r"comerc|inversi[oó]n|aduana|fiscal|tributari|libre\s+comercio|tlcan|t-mec|"
    r"financ|econ[oó]mic|empresa|mercanc[ií]a|propiedad\s+intelectual|tributo|"
    r"impuesto|doble\s+(?:imposición|tributación)",
    re.I,
)

# Strip HTML tags
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def text_only(s: str) -> str:
    s = TAG_RE.sub(" ", s)
    s = (s.replace("&nbsp;", " ")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&aacute;", "á").replace("&eacute;", "é").replace("&iacute;", "í")
            .replace("&oacute;", "ó").replace("&uacute;", "ú").replace("&ntilde;", "ñ"))
    return WS_RE.sub(" ", s).strip()


def parse_html(html: str) -> dict:
    out: dict = {}

    # Title: SRE puts it in <h4> inside div.container.servicios. Try h4 first, fall back.
    for tag in ["h4", "h3", "h2", "h1", "h5"]:
        for m in re.finditer(rf"<{tag}[^>]*>(.*?)</{tag}>", html, re.S | re.I):
            t = text_only(m.group(1))
            if t and len(t) > 8 and "Biblioteca" not in t and "Preguntas" not in t and "Servicios" not in t:
                out["nombre"] = t
                break
        if out.get("nombre"):
            break

    # Field labels: find each <b>LABEL:</b> and capture everything until the
    # next <b>, the closing of its parent block, or another known field label.
    # Strategy: find all label positions with span, then for each label, slice
    # the html between this label's end and the next label's start; clean it.
    label_positions: list[tuple[str, int, int]] = []
    for m in re.finditer(r"<b[^>]*>([^<]+?)</b>", html, re.I):
        raw_label = m.group(1).strip()
        key = LABEL_MAP.get(raw_label.rstrip(":").lower())
        if key:
            label_positions.append((key, m.start(), m.end()))

    for i, (key, start, end) in enumerate(label_positions):
        next_start = label_positions[i + 1][1] if i + 1 < len(label_positions) else len(html)
        segment = html[end:next_start]
        # Stop at the first closing block tag boundary that suggests end-of-card
        # (i.e. </div></div> or a sibling </tr>). Keep it lenient.
        # We also cut at the appearance of a known phrase that suggests a sibling block.
        m_cut = re.search(r"</tr>|</table>|</section>|</aside>|<hr|<footer", segment, re.I)
        if m_cut:
            segment = segment[:m_cut.start()]
        val = text_only(segment).strip(" :;,.\n\t")
        if val:
            out[key] = val

    return out


def infer_tipo(*texts: str | None) -> str:
    blob = " ".join(t for t in texts if t).lower()
    if DDHH_HINTS.search(blob):
        return "ddhh"
    if COMMERCIAL_HINTS.search(blob):
        return "comercial"
    return "otro"


def main():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT token_sre, detail_html FROM sre_tratados WHERE detail_html IS NOT NULL"
    ).fetchall()
    print(f"re-parsing {len(rows)} rows")

    n_with_nombre = 0
    n_with_cat = 0
    n_with_vigor = 0
    for tok, html in rows:
        p = parse_html(html)
        if p.get("nombre"):
            n_with_nombre += 1
        if p.get("categoria"):
            n_with_cat += 1
        if p.get("fecha_vigor_mexico"):
            n_with_vigor += 1
        tipo = infer_tipo(p.get("categoria"), p.get("tema"), p.get("nombre"))
        conn.execute(
            """UPDATE sre_tratados SET
                  nombre=?,
                  categoria=?, tema=?, estatus=?,
                  lugar_adopcion=?,
                  fecha_adopcion=?,
                  aprobacion_senado=?, publ_dof_aprobacion=?,
                  fecha_vigor_mexico=?, publ_dof_promulgacion=?,
                  tramite_constitucional=?, notas=?,
                  tipo=?
               WHERE token_sre=?""",
            (
                p.get("nombre"),
                p.get("categoria"), p.get("tema"), p.get("estatus"),
                p.get("lugar_adopcion"),
                p.get("lugar_adopcion"),  # fecha_adopcion approximated to combined string
                p.get("aprobacion_senado"), p.get("publ_dof_aprobacion"),
                p.get("fecha_vigor_mexico"), p.get("publ_dof_promulgacion"),
                p.get("tramite_constitucional"), p.get("notas"),
                tipo, tok,
            ),
        )
    conn.commit()

    print(f"\n  with nombre:   {n_with_nombre}")
    print(f"  with categoria:{n_with_cat}")
    print(f"  with vigor:    {n_with_vigor}")

    # Distribution
    rows = conn.execute("SELECT tipo, COUNT(*) FROM sre_tratados WHERE tipo IS NOT NULL GROUP BY tipo ORDER BY 2 DESC").fetchall()
    print("\n  tipo distribution:")
    for tipo, n in rows:
        print(f"    {tipo}: {n}")


if __name__ == "__main__":
    main()
