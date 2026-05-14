"""TSJ estatales — probe wp-json APIs across 32 state portals."""
import asyncio, httpx, json
from pathlib import Path

OUT = Path(__file__).parent / "tsj_recon.json"

# Best-known domain candidates per state (some have multiple aliases)
TSJ_DOMAINS = {
    "AGS": ["poderjudicialags.gob.mx"],
    "BC":  ["pjbc.gob.mx"],
    "BCS": ["tsj.bcs.gob.mx", "tsjbcs.gob.mx", "poderjudicialbcs.gob.mx"],
    "CAM": ["poderjudicialcampeche.gob.mx"],
    "CHIS":["poderjudicialchiapas.gob.mx"],
    "CHIH":["stj.chihuahua.gob.mx", "stjchihuahua.gob.mx"],
    "CDMX":["poderjudicialcdmx.gob.mx"],
    "COAH":["pjecz.gob.mx"],
    "COL": ["stjcolima.gob.mx"],
    "DGO": ["tsjdgo.gob.mx", "pjedurango.gob.mx"],
    "EDOMEX":["pjedomex.gob.mx"],
    "GTO": ["poderjudicial-gto.gob.mx"],
    "GRO": ["tsj-guerrero.gob.mx", "poderjudicialguerrero.gob.mx"],
    "HGO": ["pjhidalgo.gob.mx"],
    "JAL": ["tsjjalisco.gob.mx", "stjjalisco.gob.mx"],
    "MICH":["poderjudicialmichoacan.gob.mx"],
    "MOR": ["tsjmorelos.gob.mx"],
    "NAY": ["tsjnay.gob.mx"],
    "NL":  ["pjenl.gob.mx"],
    "OAX": ["tribunaloaxaca.gob.mx"],
    "PUE": ["htsjpuebla.gob.mx"],
    "QRO": ["tsjqro.gob.mx"],
    "QROO":["tsjqroo.gob.mx"],
    "SLP": ["stjslp.gob.mx"],
    "SIN": ["stj-sin.gob.mx", "stjsinaloa.gob.mx"],
    "SON": ["stjsonora.gob.mx"],
    "TAB": ["tsj-tab.gob.mx"],
    "TAMS":["pjetam.gob.mx"],
    "TLAX":["tsjtlaxcala.gob.mx"],
    "VER": ["pjeveracruz.gob.mx"],
    "YUC": ["tsjyuc.gob.mx"],
    "ZAC": ["tsjzac.gob.mx"],
}

HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/131.0"}


async def probe_state(state: str, domains: list[str], client: httpx.AsyncClient) -> dict:
    out = {"state": state, "ok_domain": None, "platform": None, "endpoints": {}}
    for dom in domains:
        for scheme in ("https://www.", "https://", "https://www2."):
            url = f"{scheme}{dom}/"
            try:
                r = await client.get(url, timeout=10, follow_redirects=True)
                if r.status_code == 200 and len(r.text) > 500:
                    out["ok_domain"] = r.url.host
                    body_head = r.text[:5000].lower()
                    # Detect platform
                    if "wp-json" in body_head or "wp-content" in body_head:
                        out["platform"] = "wordpress"
                    elif "data-beasties-container" in body_head or "_ngcontent" in body_head:
                        out["platform"] = "angular"
                    elif "asp.net" in body_head or "viewstate" in body_head:
                        out["platform"] = "aspnet"
                    elif "drupal" in body_head:
                        out["platform"] = "drupal"
                    elif "joomla" in body_head:
                        out["platform"] = "joomla"
                    else:
                        out["platform"] = "other"
                    # If wordpress, probe wp-json
                    if out["platform"] == "wordpress":
                        try:
                            wpr = await client.get(f"{scheme}{dom}/wp-json/", timeout=10)
                            if wpr.status_code == 200:
                                out["endpoints"]["wp-json"] = True
                                # try /types
                                tr = await client.get(f"{scheme}{dom}/wp-json/wp/v2/types", timeout=10)
                                if tr.status_code == 200:
                                    try:
                                        types_data = tr.json()
                                        # Filter for content-bearing post types
                                        custom = [k for k in types_data if k not in ("post","page","attachment","nav_menu_item","wp_block","wp_template","wp_template_part","wp_navigation")]
                                        out["endpoints"]["custom_post_types"] = custom
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                    return out
            except Exception:
                continue
    return out


async def main():
    results = []
    async with httpx.AsyncClient(http2=False, headers=HEADERS, verify=False) as client:
        tasks = [probe_state(s, d, client) for s, d in TSJ_DOMAINS.items()]
        results = await asyncio.gather(*tasks)

    OUT.write_text(json.dumps(results, indent=2, ensure_ascii=False))

    # Summary
    by_platform = {}
    for r in results:
        p = r["platform"] or "unreachable"
        by_platform.setdefault(p, []).append(r["state"])
    print("Platform summary:")
    for p, states in by_platform.items():
        print(f"  {p}: {len(states)} → {','.join(states)}")
    print("\nWordPress states with custom post types:")
    for r in results:
        if r["platform"] == "wordpress" and r["endpoints"].get("custom_post_types"):
            print(f"  {r['state']:6s} @ {r['ok_domain']}")
            for cpt in r["endpoints"]["custom_post_types"][:8]:
                print(f"     - {cpt}")
    print(f"\nFull recon: {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
