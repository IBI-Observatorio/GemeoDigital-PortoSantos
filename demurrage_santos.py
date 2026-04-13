import json
import urllib.request
from datetime import datetime, timezone

DEMURRAGE_RATE = 29_800  # USD/dia (proxy técnica para Panamax ~75.000 DWT)
API_URL = "https://scraper-santos.up.railway.app/api/fundeados"
NOW = datetime.now(timezone.utc)

# Cargas de combustível/gás a filtrar (cabotagem de abastecimento)
COMBUSTIVEIS = {
    "GASOLINA COMUM", "OLEO DIESEL", "GAS LIQUEFEITO",
    "OLEO COMBUSTIVEL",
}

NAVIOS_EXCLUIR = {"GUAJARA", "TS 4"}


def fetch_ships():
    req = urllib.request.Request(API_URL, headers={"User-Agent": "demurrage-calc/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    return data.get("ships", data) if isinstance(data, dict) else data


def parse_arrival(dt_str):
    for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def is_programado(ship):
    name = ship.get("vessel_name", "").strip().upper()
    if name in NAVIOS_EXCLUIR:
        return False
    cargo = ship.get("cargo_type", "").strip().upper()
    if cargo in COMBUSTIVEIS:
        return False
    if cargo == "VEICULO" and ship.get("navigation") == "Cab":
        return False
    if not cargo and not ship.get("operation"):
        return False
    return True


def calc_demurrage():
    ships = fetch_ships()
    programados = [s for s in ships if is_programado(s)]

    results = []
    for s in programados:
        arrival = parse_arrival(s.get("arrival", ""))
        if not arrival:
            continue
        wait_hours = (NOW - arrival).total_seconds() / 3600
        wait_days = max(wait_hours / 24, 0)
        demurrage = wait_days * DEMURRAGE_RATE
        results.append({
            "navio": s["vessel_name"],
            "bandeira": s.get("flag", ""),
            "carga": s.get("cargo_type", ""),
            "tonelagem": s.get("weight_tons", 0),
            "chegada": s.get("arrival", ""),
            "dias_espera": round(wait_days, 1),
            "demurrage_usd": round(demurrage),
        })

    results.sort(key=lambda r: r["dias_espera"], reverse=True)
    return results


def print_report(results):
    print("=" * 110)
    print("  DEMURRAGE DOS NAVIOS FUNDEADOS NO PORTO DE SANTOS")
    print(f"  Data de referência: {NOW.strftime('%d/%m/%Y %H:%M')} UTC")
    print(f"  Taxa de demurrage: USD {DEMURRAGE_RATE:,.0f}/dia")
    print("=" * 110)
    print()

    hdr = f"{'Navio':<25} {'Carga':<22} {'Ton':>8} {'Chegada':<18} {'Dias':>6} {'Demurrage (USD)':>16}"
    print(hdr)
    print("-" * 110)

    total_demurrage = 0
    total_dias = 0
    for r in results:
        tons = r["tonelagem"]
        tons_str = f"{tons:>8,}" if isinstance(tons, (int, float)) and tons > 0 else f"{'—':>8}"
        print(
            f"{r['navio']:<25} {r['carga']:<22} {tons_str} "
            f"{r['chegada']:<18} {r['dias_espera']:>6.1f} {r['demurrage_usd']:>16,}"
        )
        total_demurrage += r["demurrage_usd"]
        total_dias += r["dias_espera"]

    n = len(results)
    avg_dias = total_dias / n if n else 0

    print("-" * 110)
    print(f"{'TOTAL':.<25} {n} navios{' ' * 43} {avg_dias:>6.1f} {total_demurrage:>16,}")
    print()

    # Agrupamento por tipo de carga
    by_cargo = {}
    for r in results:
        cargo = r["carga"] or "SEM CARGA"
        if cargo not in by_cargo:
            by_cargo[cargo] = {"count": 0, "demurrage": 0, "dias": 0}
        by_cargo[cargo]["count"] += 1
        by_cargo[cargo]["demurrage"] += r["demurrage_usd"]
        by_cargo[cargo]["dias"] += r["dias_espera"]

    print("  RESUMO POR TIPO DE CARGA")
    print("-" * 70)
    print(f"  {'Tipo de Carga':<30} {'Navios':>7} {'Dias Méd.':>10} {'Demurrage (USD)':>18}")
    print("-" * 70)
    for cargo in sorted(by_cargo, key=lambda c: by_cargo[c]["demurrage"], reverse=True):
        info = by_cargo[cargo]
        avg = info["dias"] / info["count"] if info["count"] else 0
        print(f"  {cargo:<30} {info['count']:>7} {avg:>10.1f} {info['demurrage']:>18,}")
    print("-" * 70)
    print(f"  {'TOTAL':<30} {n:>7} {avg_dias:>10.1f} {total_demurrage:>18,}")
    print()


if __name__ == "__main__":
    results = calc_demurrage()
    print_report(results)
