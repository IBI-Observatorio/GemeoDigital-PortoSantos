"""
Scraper de navios — Porto de Santos
Faz scraping das páginas oficiais da SPA de tempos em tempos e serve
os dados em JSON via HTTP local para o index.html consumir.

Uso:
  pip install requests beautifulsoup4 anthropic
  python scraper.py

Endpoints:
  GET  http://localhost:8080/api/atracados   → JSON com navios atracados
  GET  http://localhost:8080/api/fundeados   → JSON com navios fundeados
  GET  http://localhost:8080/api/demurrage   → JSON com cálculo de demurrage
  POST http://localhost:8080/api/ask         → Pergunta ao Gêmeo AI (Claude)
  GET  http://localhost:8080/api/status      → status do scraper
"""

import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

URL_ATRACADOS = 'https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/atracados-porto-terminais/'
URL_FUNDEADOS = 'https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-fundeados/'

DEMURRAGE_RATE = 29_800  # USD/dia (proxy Panamax ~75.000 DWT)
COMBUSTIVEIS = {"GASOLINA COMUM", "OLEO DIESEL", "GAS LIQUEFEITO", "OLEO COMBUSTIVEL"}
NAVIOS_EXCLUIR = {"GUAJARA", "TS 4"}
INTERVAL_SECONDS = 10 * 60  # 10 minutos
PORT = int(os.environ.get('PORT', 8080))

# Mapeamento de berço → terminal (para posicionamento no mapa).
# Ordem importa: prefixos mais longos primeiro para evitar match errado
# (ex: "TEAG" antes de "TEG", "ARMAZEM 39" antes de "ARMAZEM").
BERTH_TO_TERMINAL = [
    ('TECON',       'SANTOS_BRASIL'),
    ('BTP',         'BTP_SANTOS'),
    ('EPORT',       'ECOPORTO'),
    ('TERM. DOW',   'DOW'),
    ('TERMAG',      'TERMAG'),
    ('TEAG',        'TEAG'),
    ('TEG',         'TEG'),
    ('TEV',         'TEV'),
    ('TES',         'TES'),
    ('TGG',         'TGG'),
    ('CUTRALE',     'CUTRALE'),
    ('ADM',         'ADM'),
    ('CONCAIS',     'CONCAIS'),
    ('ARMAZEM 39',  'ARMAZEM_39'),
    ('ARMAZEM 37',  'ARMAZEM_37'),
    ('ARMAZEM 20',  'ARMAZEM_20_21'),
    ('ARMAZEM 21',  'ARMAZEM_20_21'),
]

def match_terminal(berth_name):
    upper = berth_name.upper().strip()
    for prefix, terminal in BERTH_TO_TERMINAL:
        if upper.startswith(prefix.upper()):
            return terminal
    return 'OUTRO'

# ── Estado global ──
data_store = {
    'atracados': { 'ships': [], 'total': 0, 'last_update': None, 'last_error': None },
    'fundeados': { 'ships': [], 'total': 0, 'last_update': None, 'last_error': None },
    'update_count': 0,
}
lock = threading.Lock()


def scrape_atracados():
    """Faz scraping da tabela de atracados."""
    resp = requests.get(URL_ATRACADOS, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=30)
    resp.raise_for_status()
    resp.encoding = 'utf-8'

    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table')
    if not table:
        raise ValueError('Tabela de atracados não encontrada na página')

    rows = table.find_all('tr')
    ships = []

    for row in rows[1:]:
        cells = row.find_all('td')
        if len(cells) < 8:
            continue

        berth = cells[0].get_text(strip=True)
        vessel = cells[1].get_text(strip=True)
        if not vessel:
            continue

        cargo = cells[6].get_text(strip=True)
        unload = cells[7].get_text(strip=True)
        load = cells[8].get_text(strip=True) if len(cells) > 8 else '0'

        try:
            unload_num = int(re.sub(r'[^\d]', '', unload)) if unload else 0
        except ValueError:
            unload_num = 0
        try:
            load_num = int(re.sub(r'[^\d]', '', load)) if load else 0
        except ValueError:
            load_num = 0

        ships.append({
            'berth': berth,
            'vessel_name': vessel,
            'cargo': cargo,
            'unload_tons': unload_num,
            'load_tons': load_num,
            'terminal': match_terminal(berth),
        })

    return ships


def scrape_fundeados():
    """Faz scraping da tabela de fundeados."""
    resp = requests.get(URL_FUNDEADOS, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=30)
    resp.raise_for_status()
    resp.encoding = 'utf-8'

    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table')
    if not table:
        raise ValueError('Tabela de fundeados não encontrada na página')

    rows = table.find_all('tr')
    ships = []

    for row in rows[1:]:
        cells = row.find_all('td')
        if len(cells) < 12:
            continue

        vessel_raw = cells[0].get_text(strip=True)
        if not vessel_raw:
            continue

        # Remover sufixo "PROGRAMADO" que aparece colado ao nome
        vessel = re.sub(r'PROGRAMADO$', '', vessel_raw, flags=re.I).strip()

        flag = cells[1].get_text(strip=True)

        # Comprimento e calado vêm juntos (ex: "22913" = 229m, 13m calado)
        len_draft_raw = cells[2].get_text(strip=True)
        length = None
        draft = None
        m = re.match(r'(\d{2,3})([\d.]+)', len_draft_raw)
        if m:
            length = int(m.group(1))
            try:
                draft = float(m.group(2))
            except ValueError:
                pass

        nav = cells[3].get_text(strip=True)  # Cab = cabotagem, Long = longo curso
        arrival = cells[4].get_text(strip=True)
        notice = cells[5].get_text(strip=True)
        agency = cells[6].get_text(strip=True)
        operation = cells[7].get_text(strip=True)  # EMB = embarque, DESC = descarga
        cargo_type = cells[8].get_text(strip=True)

        weight_raw = cells[9].get_text(strip=True)
        try:
            weight = int(re.sub(r'[^\d]', '', weight_raw)) if weight_raw else 0
        except ValueError:
            weight = 0

        voyage = cells[10].get_text(strip=True)
        priority = cells[11].get_text(strip=True)  # A, B, etc.
        terminal_dest = cells[12].get_text(strip=True) if len(cells) > 12 else ''

        ships.append({
            'vessel_name': vessel,
            'flag': flag,
            'length': length,
            'draft': draft,
            'navigation': nav,
            'arrival': arrival,
            'notice': notice,
            'agency': agency,
            'operation': operation,
            'cargo_type': cargo_type,
            'weight_tons': weight,
            'voyage': voyage,
            'priority': priority,
            'terminal_dest': terminal_dest,
        })

    return ships


def calc_demurrage(fundeados):
    """Calcula demurrage dos navios fundeados."""
    now = datetime.now(timezone.utc)
    results = []

    for s in fundeados:
        name = s.get('vessel_name', '').strip().upper()
        if name in NAVIOS_EXCLUIR:
            continue
        cargo = s.get('cargo_type', '').strip().upper()
        if cargo in COMBUSTIVEIS:
            continue
        if cargo == 'VEICULO' and s.get('navigation') == 'Cab':
            continue
        if not cargo and not s.get('operation'):
            continue

        arrival_str = s.get('arrival', '')
        arrival = None
        for fmt in ('%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
            try:
                arrival = datetime.strptime(arrival_str, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
        if not arrival:
            continue

        wait_days = max((now - arrival).total_seconds() / 86400, 0)
        demurrage = wait_days * DEMURRAGE_RATE

        results.append({
            'navio': s['vessel_name'],
            'bandeira': s.get('flag', ''),
            'carga': s.get('cargo_type', ''),
            'tonelagem': s.get('weight_tons', 0),
            'chegada': arrival_str,
            'dias_espera': round(wait_days, 1),
            'demurrage_usd': round(demurrage),
        })

    results.sort(key=lambda r: r['dias_espera'], reverse=True)

    total_demurrage = sum(r['demurrage_usd'] for r in results)
    total_dias = sum(r['dias_espera'] for r in results)
    n = len(results)
    avg_dias = round(total_dias / n, 1) if n else 0

    by_cargo = {}
    for r in results:
        cargo = r['carga'] or 'SEM CARGA'
        if cargo not in by_cargo:
            by_cargo[cargo] = {'count': 0, 'demurrage': 0, 'dias': 0}
        by_cargo[cargo]['count'] += 1
        by_cargo[cargo]['demurrage'] += r['demurrage_usd']
        by_cargo[cargo]['dias'] += r['dias_espera']

    resumo_cargo = []
    for cargo in sorted(by_cargo, key=lambda c: by_cargo[c]['demurrage'], reverse=True):
        info = by_cargo[cargo]
        resumo_cargo.append({
            'carga': cargo,
            'navios': info['count'],
            'dias_medio': round(info['dias'] / info['count'], 1) if info['count'] else 0,
            'demurrage_usd': info['demurrage'],
        })

    return {
        'navios': results,
        'total_navios': n,
        'total_demurrage_usd': total_demurrage,
        'media_dias_espera': avg_dias,
        'taxa_diaria_usd': DEMURRAGE_RATE,
        'por_carga': resumo_cargo,
        'calculado_em': now.isoformat(),
    }


SYSTEM_PROMPT = """Você é o Gêmeo AI, assistente inteligente do Porto de Santos desenvolvido pelo Instituto Brasileiro de Infraestrutura (IBI).

Você responde perguntas sobre:
- Navios atracados e fundeados no Porto de Santos (dados em tempo real)
- Demurrage estimado dos navios fundeados
- Emprego marítimo e portuário na Baixada Santista (RAIS 2024 + CAGED 2025-2026)

REGRAS:
- Responda sempre em português do Brasil
- Seja conciso e direto
- Use dados numéricos quando disponíveis
- Para emprego, use apenas PERCENTUAIS, nunca números absolutos de vínculos
- Cite a fonte (RAIS 2024, CAGED 2025-2026, APS tempo real)
- Se não souber, diga que não tem a informação

DADOS DE EMPREGO (Relatório IBI — RAIS 2024):
- Complexo Portuário: 4 municípios (Santos, Guarujá, Cubatão, São Vicente), 23 CNAEs
- Santos concentra 96% dos vínculos
- Gênero: 60,5% masculino / 39,5% feminino
- Escolaridade: Médio Completo 45,7% | Superior Completo 34,4% | 85% com Médio+
- Remuneração mediana: R$ 3.264 | Superior ganha 3,4x mais que Médio (+243%)
- Faixa etária: 30-39 anos = 30% (maior) | 18-24 anos = 11,4% (baixa entrada jovens)
- Evolução: 2021→2022 +11,7% | 2022→2023 +11,5% | 2023→2024 +0,2%
- CAGED 2025-2026: saldo positivo em 13 de 14 meses
- Por atividade: Serviços e Apoio 73,5% | Transporte 19,6% | Armazenagem 3,4% | Obras 2,7% | Operações Portuárias 0,8%
- Remuneração por atividade: Op. Portuárias R$ 6.921 | Armazenagem R$ 3.820 | Obras R$ 3.727 | Serviços R$ 3.255 | Transporte R$ 3.096
- Correlação escolaridade/renda: salto forte do Médio para Superior (+243%), não linear
- Taxa demurrage: USD 29.800/dia (proxy Panamax ~75.000 DWT)
"""


def ask_claude(question, atracados, fundeados, demurrage_data):
    """Envia pergunta ao Claude Haiku com contexto dos dados do porto."""
    if not HAS_ANTHROPIC:
        return 'Erro: biblioteca anthropic não instalada. pip install anthropic'

    # Montar contexto com dados em tempo real
    ctx_parts = []

    if atracados:
        ships_list = ', '.join(s['vessel_name'] + ' (' + s['berth'] + ', ' + (s.get('cargo','') or '—') + ')'
                               for s in atracados[:30])
        ctx_parts.append(f"NAVIOS ATRACADOS AGORA ({len(atracados)} total): {ships_list}")

    if fundeados:
        fund_list = ', '.join(s['vessel_name'] + ' (' + (s.get('cargo_type','') or '—') + ', ' + str(s.get('dias_espera','?')) + ' dias)'
                              for s in fundeados[:20])
        ctx_parts.append(f"NAVIOS FUNDEADOS AGORA ({len(fundeados)} total): {fund_list}")

    if demurrage_data:
        ctx_parts.append(f"DEMURRAGE TOTAL: USD {demurrage_data.get('total_demurrage_usd',0):,.0f} | "
                        f"{demurrage_data.get('total_navios',0)} navios | "
                        f"espera média {demurrage_data.get('media_dias_espera',0)} dias")
        if demurrage_data.get('por_carga'):
            cargo_lines = ', '.join(c['carga'] + ': ' + str(c['navios']) + ' navios USD ' + f"{c['demurrage_usd']:,}"
                                    for c in demurrage_data['por_carga'][:5])
            ctx_parts.append(f"DEMURRAGE POR CARGA: {cargo_lines}")

    context = '\n'.join(ctx_parts)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[
            {'role': 'user', 'content': f"DADOS EM TEMPO REAL DO PORTO:\n{context}\n\nPERGUNTA DO USUÁRIO: {question}"}
        ]
    )

    return message.content[0].text


def scrape_loop():
    """Loop de scraping que roda em background."""
    while True:
        now = datetime.now(timezone.utc).isoformat()

        # Atracados
        try:
            ships = scrape_atracados()
            with lock:
                data_store['atracados']['ships'] = ships
                data_store['atracados']['total'] = len(ships)
                data_store['atracados']['last_update'] = now
                data_store['atracados']['last_error'] = None
            print(f'[{now[:19]}] Atracados OK: {len(ships)} navios')
        except Exception as e:
            with lock:
                data_store['atracados']['last_error'] = str(e)
            print(f'[{now[:19]}] Atracados ERRO: {e}')

        # Fundeados
        try:
            ships = scrape_fundeados()
            with lock:
                data_store['fundeados']['ships'] = ships
                data_store['fundeados']['total'] = len(ships)
                data_store['fundeados']['last_update'] = now
                data_store['fundeados']['last_error'] = None
            print(f'[{now[:19]}] Fundeados OK: {len(ships)} navios')
        except Exception as e:
            with lock:
                data_store['fundeados']['last_error'] = str(e)
            print(f'[{now[:19]}] Fundeados ERRO: {e}')

        with lock:
            data_store['update_count'] += 1

        time.sleep(INTERVAL_SECONDS)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path

        if path == '/api/atracados':
            with lock:
                body = json.dumps(data_store['atracados'], ensure_ascii=False)
            self._respond(200, body)

        elif path == '/api/fundeados':
            with lock:
                body = json.dumps(data_store['fundeados'], ensure_ascii=False)
            self._respond(200, body)

        elif path == '/api/demurrage':
            with lock:
                fundeados = data_store['fundeados'].get('ships', [])
            demurrage = calc_demurrage(fundeados)
            self._respond(200, json.dumps(demurrage, ensure_ascii=False))

        elif path == '/api/status':
            with lock:
                status = {
                    'atracados': data_store['atracados']['total'],
                    'fundeados': data_store['fundeados']['total'],
                    'last_update_atracados': data_store['atracados']['last_update'],
                    'last_update_fundeados': data_store['fundeados']['last_update'],
                    'last_error_atracados': data_store['atracados']['last_error'],
                    'last_error_fundeados': data_store['fundeados']['last_error'],
                    'update_count': data_store['update_count'],
                    'interval_seconds': INTERVAL_SECONDS,
                }
            self._respond(200, json.dumps(status, ensure_ascii=False))

        else:
            self._respond(404, json.dumps({'error': 'not found'}))

    def do_POST(self):
        path = urlparse(self.path).path

        if path == '/api/ask':
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            try:
                payload = json.loads(body)
                question = payload.get('question', '').strip()
                if not question:
                    self._respond(400, json.dumps({'error': 'question is required'}))
                    return

                with lock:
                    atracados = data_store['atracados'].get('ships', [])
                    fundeados = data_store['fundeados'].get('ships', [])

                demurrage_data = calc_demurrage(fundeados)
                answer = ask_claude(question, atracados, fundeados, demurrage_data)

                self._respond(200, json.dumps({'answer': answer}, ensure_ascii=False))
            except Exception as e:
                self._respond(500, json.dumps({'error': str(e)}, ensure_ascii=False))
        else:
            self._respond(404, json.dumps({'error': 'not found'}))

    def _respond(self, code, body):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def do_OPTIONS(self):
        self._respond(204, '')

    def log_message(self, format, *args):
        pass


def main():
    if not HAS_DEPS:
        print('Dependencias faltando. Instale com:')
        print('  pip install requests beautifulsoup4')
        return

    print(f'Porto de Santos — Scraper de navios')
    print(f'Intervalo: {INTERVAL_SECONDS // 60} minutos')
    print(f'Endpoints:')
    print(f'  http://localhost:{PORT}/api/atracados')
    print(f'  http://localhost:{PORT}/api/fundeados')
    print(f'  http://localhost:{PORT}/api/status')
    print()

    t = threading.Thread(target=scrape_loop, daemon=True)
    t.start()

    server = HTTPServer(('0.0.0.0', PORT), Handler)
    print(f'Servidor rodando em http://localhost:{PORT}')
    print('Ctrl+C para parar\n')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nParando...')
        server.server_close()


if __name__ == '__main__':
    main()
