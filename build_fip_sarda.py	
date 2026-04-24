#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_fip_sarda.py
==================
Integra i dati FIP Sarda (classifiche/risultati) nel template HTML.
Viene chiamato da deploy.bat PRIMA di build_v5.py.

Cosa fa:
  1. Legge fip_sarda_data.json (prodotto da fip_scraper_sarda.py)
  2. Lo serializza in JSON compatto
  3. Sostituisce il placeholder __FIP_SARDA__ nel template

Uso:
  python build_fip_sarda.py [--template PERCORSO_TEMPLATE] [--data PERCORSO_JSON]

Di default lavora sul file scripts/template.html (come build_v5.py).
"""

import json
import os
import sys
import argparse
from datetime import datetime

# ── Percorsi di default (compatibili con struttura repo fpsrdg) ──
DEFAULT_TEMPLATE = os.path.join('scripts', 'template.html')
DEFAULT_DATA     = 'fip_sarda_data.json'
DEFAULT_OUTPUT   = os.path.join('scripts', 'template.html')  # sovrascrive in-place

PLACEHOLDER = '__FIP_SARDA__'

# Dato vuoto da usare se il JSON non esiste
EMPTY_DATA = {
    'aggiornato': '',
    'campionati': []
}


def log(msg):
    print(f"[build_fip_sarda] {msg}", flush=True)


def load_fip_data(path):
    if not os.path.exists(path):
        log(f"File dati non trovato: {path} — uso struttura vuota.")
        return EMPTY_DATA
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        log(f"Dati FIP caricati: {len(data.get('campionati', []))} campionati — {path}")
        return data
    except Exception as e:
        log(f"Errore lettura {path}: {e} — uso struttura vuota.")
        return EMPTY_DATA


def build(template_path, data_path, output_path):
    # Carica template
    if not os.path.exists(template_path):
        log(f"ERRORE: Template non trovato: {template_path}")
        sys.exit(1)

    with open(template_path, 'r', encoding='utf-8') as f:
        html = f.read()

    if PLACEHOLDER not in html:
        log(f"ATTENZIONE: placeholder '{PLACEHOLDER}' non trovato nel template — nessuna modifica.")
        # Scrivi output uguale all'input se template ≠ output
        if template_path != output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html)
        return

    # Carica dati
    data = load_fip_data(data_path)

    # Serializza in JSON compatto (senza spazi extra, per ridurre dimensione file)
    data_json = json.dumps(data, ensure_ascii=False, separators=(',', ':'))

    # Sostituzione
    html_out = html.replace(PLACEHOLDER, data_json, 1)

    # Salva output
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_out)

    sz = os.path.getsize(output_path)
    log(f"Template aggiornato: {output_path} ({sz:,} byte)")
    log(f"FIP Sarda: {len(data.get('campionati', []))} campionati, aggiornato: {data.get('aggiornato', '–')}")


def main():
    parser = argparse.ArgumentParser(description='Integra dati FIP Sarda nel template HTML')
    parser.add_argument('--template', default=DEFAULT_TEMPLATE,
                        help=f'Template HTML (default: {DEFAULT_TEMPLATE})')
    parser.add_argument('--data',     default=DEFAULT_DATA,
                        help=f'JSON dati FIP (default: {DEFAULT_DATA})')
    parser.add_argument('--output',   default=None,
                        help='File output (default: sovrascrive template)')
    args = parser.parse_args()

    output = args.output or args.template
    build(args.template, args.data, output)


if __name__ == '__main__':
    main()
