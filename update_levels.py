#!/usr/bin/env python3
"""
update_levels.py — Key Levels Grid · Market Data Updater
=========================================================
Source  : yfinance (gratuit, clôtures J-1, ~15 min delay)
Calcule : PDH/PDL/PWH/PWL/PMH/PML · DO/WO/MO · Sessions
Injecte : dans index.html via marqueurs MARKET_DATA
"""

import sys
import json
import re
import os
from datetime import datetime, timedelta

import pytz

PARIS_TZ = pytz.timezone('Europe/Paris')
UTC_TZ   = pytz.utc

# ================================================================
# CONFIG — tickers yfinance
# ================================================================
ASSETS = {
    'XAU': {'ticker': 'GC=F',     'label': 'XAU/USD', 'digits': 2},
    'BTC': {'ticker': 'BTC-USD',  'label': 'BTC/USD', 'digits': 0},
    'SP5': {'ticker': '^GSPC',    'label': 'SP500',   'digits': 2},
    'NAS': {'ticker': '^IXIC',    'label': 'NASDAQ',  'digits': 2},
    'WTI': {'ticker': 'CL=F',     'label': 'WTI',     'digits': 2},
    'DXY': {'ticker': 'DX-Y.NYB', 'label': 'DXY',     'digits': 3},
}

# Sessions UTC (heure_debut, heure_fin)
# Asia chevauche minuit : 22h->08h
SESSIONS = {
    'asia':   (22, 8),
    'london': (7, 16),
    'ny':     (13, 21),
}


# ================================================================
# UTILITAIRES
# ================================================================
def fmt(v, digits):
    """Arrondit proprement, retourne None si invalide."""
    try:
        return round(float(v), digits)
    except Exception:
        return None


def in_session(hour_utc, sess_key):
    """Teste si une heure UTC appartient à une fenêtre de session."""
    start, end = SESSIONS[sess_key]
    if start < end:
        return start <= hour_utc < end
    # Fenêtre qui passe minuit (Asia)
    return hour_utc >= start or hour_utc < end


def to_date_safe(idx_val):
    """Convertit un index pandas en date Python, sans lever d'exception."""
    try:
        if hasattr(idx_val, 'date'):
            return idx_val.date()
        if hasattr(idx_val, 'to_pydatetime'):
            return idx_val.to_pydatetime().date()
        return idx_val
    except Exception:
        return None


# ================================================================
# FETCH PRINCIPAL
# ================================================================
def fetch_asset(key, cfg):
    """
    Récupère tous les niveaux mécaniques pour un actif.
    Retourne un dict ou None si échec complet.
    """
    import yfinance as yf

    ticker = cfg['ticker']
    d      = cfg['digits']
    result = {}

    print(f'\n  ── {key} ({ticker}) ──')

    # ── 1. DONNÉES JOURNALIÈRES (3 mois)
    try:
        hist_d = yf.Ticker(ticker).history(period='3mo', interval='1d')
        print(f'     Daily rows : {len(hist_d)}')

        if hist_d.empty:
            print(f'     ERREUR : aucune donnée journalière.')
            return None

        # Prix actuel = dernière clôture
        result['price'] = fmt(hist_d['Close'].iloc[-1], d)

        # PDH / PDL = J-1
        idx = -2 if len(hist_d) >= 2 else -1
        result['pdh'] = fmt(hist_d['High'].iloc[idx], d)
        result['pdl'] = fmt(hist_d['Low'].iloc[idx],  d)

        # DO = open du dernier jour
        result['do'] = fmt(hist_d['Open'].iloc[-1], d)

        # WO = open du premier trading day de la semaine courante
        today      = datetime.now(UTC_TZ).date()
        week_start = today - timedelta(days=today.weekday())
        wo = None
        for i in range(len(hist_d) - 1, -1, -1):
            row_date = to_date_safe(hist_d.index[i])
            if row_date is None:
                continue
            if row_date >= week_start:
                wo = fmt(hist_d['Open'].iloc[i], d)
            else:
                break
        result['wo'] = wo

        # MO = open du premier trading day du mois courant
        month_start = today.replace(day=1)
        mo = None
        for i in range(len(hist_d) - 1, -1, -1):
            row_date = to_date_safe(hist_d.index[i])
            if row_date is None:
                continue
            if row_date >= month_start:
                mo = fmt(hist_d['Open'].iloc[i], d)
            else:
                break
        result['mo'] = mo

        print(f'     price={result["price"]} | PDH={result["pdh"]} '
              f'PDL={result["pdl"]} | DO={result["do"]} '
              f'WO={result["wo"]} MO={result["mo"]}')

    except Exception as e:
        print(f'     ERREUR daily : {e}')
        return None

    # ── 2. DONNÉES HEBDOMADAIRES
    try:
        hist_w = yf.Ticker(ticker).history(period='6mo', interval='1wk')
        print(f'     Weekly rows: {len(hist_w)}')
        if not hist_w.empty and len(hist_w) >= 2:
            result['pwh'] = fmt(hist_w['High'].iloc[-2], d)
            result['pwl'] = fmt(hist_w['Low'].iloc[-2],  d)
        else:
            result['pwh'] = result['pwl'] = None
        print(f'     PWH={result["pwh"]} PWL={result["pwl"]}')
    except Exception as e:
        print(f'     ERREUR weekly : {e}')
        result['pwh'] = result['pwl'] = None

    # ── 3. DONNÉES MENSUELLES
    try:
        hist_m = yf.Ticker(ticker).history(period='12mo', interval='1mo')
        print(f'     Monthly rows: {len(hist_m)}')
        if not hist_m.empty and len(hist_m) >= 2:
            result['pmh'] = fmt(hist_m['High'].iloc[-2], d)
            result['pml'] = fmt(hist_m['Low'].iloc[-2],  d)
        else:
            result['pmh'] = result['pml'] = None
        print(f'     PMH={result["pmh"]} PML={result["pml"]}')
    except Exception as e:
        print(f'     ERREUR monthly : {e}')
        result['pmh'] = result['pml'] = None

    # ── 4. DONNÉES SESSIONS (horaire 1h, 2 jours)
    for sess in ('asia', 'london', 'ny'):
        result[f'{sess}_h'] = None
        result[f'{sess}_l'] = None

    try:
        hist_1h = yf.Ticker(ticker).history(period='2d', interval='1h')
        print(f'     Hourly rows: {len(hist_1h)}')

        if not hist_1h.empty:
            buckets = {s: {'highs': [], 'lows': []} for s in SESSIONS}

            for idx_val, row in hist_1h.iterrows():
                try:
                    dt = idx_val
                    if hasattr(dt, 'to_pydatetime'):
                        dt = dt.to_pydatetime()
                    if dt.tzinfo is None:
                        dt = UTC_TZ.localize(dt)
                    else:
                        dt = dt.astimezone(UTC_TZ)
                    h = dt.hour
                    for sess in SESSIONS:
                        if in_session(h, sess):
                            buckets[sess]['highs'].append(float(row['High']))
                            buckets[sess]['lows'].append(float(row['Low']))
                except Exception:
                    continue

            for sess in SESSIONS:
                highs = buckets[sess]['highs']
                lows  = buckets[sess]['lows']
                result[f'{sess}_h'] = fmt(max(highs), d) if highs else None
                result[f'{sess}_l'] = fmt(min(lows),  d) if lows  else None

            print(f'     Asia  {result["asia_h"]}/{result["asia_l"]} | '
                  f'London {result["london_h"]}/{result["london_l"]} | '
                  f'NY {result["ny_h"]}/{result["ny_l"]}')

    except Exception as e:
        print(f'     ERREUR sessions : {e}')

    return result


# ================================================================
# BUILD + INJECT
# ================================================================
def build_market_data():
    now = datetime.now(PARIS_TZ)
    md  = {
        'generated_date': now.strftime('%Y-%m-%d'),
        'generated_time': now.strftime('%H:%M'),
        'assets': {}
    }
    for key, cfg in ASSETS.items():
        data = fetch_asset(key, cfg)
        if data:
            md['assets'][key] = data
    return md


def inject_html(md):
    html_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'index.html'
    )
    if not os.path.exists(html_path):
        raise FileNotFoundError(f'index.html introuvable : {html_path}')

    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()

    data_json = json.dumps(md, indent=2, ensure_ascii=False)
    block = (
        f'<!-- MARKET_DATA:START -->\n'
        f'<script id="auto-market-data">\n'
        f'/* Auto — {md["generated_date"]} {md["generated_time"]} Paris */\n'
        f'const AUTO_MARKET_DATA = {data_json};\n'
        f'applyMarketData();\n'
        f'</script>\n'
        f'<!-- MARKET_DATA:END -->'
    )

    pattern  = r'<!-- MARKET_DATA:START -->.*?<!-- MARKET_DATA:END -->'
    new_html, count = re.subn(pattern, block, html, flags=re.DOTALL)

    if count == 0:
        print('ERREUR : marqueurs MARKET_DATA:START/END introuvables dans index.html !')
        print('Vérifie que le bon fichier HTML est dans le repo.')
        sys.exit(1)

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(new_html)

    print(f'\n  index.html mis à jour ({count} bloc remplacé).')


# ================================================================
# MAIN
# ================================================================
def main():
    sep = '=' * 56
    print(sep)
    print('KEY LEVELS GRID — Market Data Updater')
    now = datetime.now(PARIS_TZ)
    print(f'Paris : {now.strftime("%Y-%m-%d %H:%M")}')
    print(sep)

    # Vérifier yfinance
    try:
        import yfinance as yf
        print(f'yfinance version : {yf.__version__}')
    except ImportError:
        print('ERREUR : yfinance non installé.')
        sys.exit(1)

    md = build_market_data()
    n  = len(md['assets'])
    total = len(ASSETS)

    print(f'\n{sep}')
    print(f'Actifs récupérés : {n}/{total}')

    for key in ASSETS:
        status = '✓' if key in md['assets'] else '✗'
        print(f'  [{status}] {key}')

    if n == 0:
        print('\nERREUR : aucun actif récupéré. index.html non modifié.')
        sys.exit(1)

    inject_html(md)
    print(f'\nTerminé — {md["generated_date"]} {md["generated_time"]}')


if __name__ == '__main__':
    main()
