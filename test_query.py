#!/usr/bin/env python3
"""Teste rapido da query"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os

DB_CONFIG = {
    'host': 'bdunicoprecs.c50cwuocuwro.sa-east-1.rds.amazonaws.com',
    'port': 5432,
    'user': 'postgres',
    'password': '$P^iFe27^YP5cpBU3J&tqa',
    'database': 'OCSC'
}

print("Testando query com campos do app.py...")

try:
    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor, connect_timeout=10)
    cursor = conn.cursor()

    # Query exatamente como no app.py
    fields = [
        'id', 'precatorio', 'ordem', 'organizacao', 'prioridade', 'tribunal',
        'natureza', 'data_base', 'situacao', 'esta_na_ordem',
        'nao_esta_na_ordem', 'ano_orc', 'valor', 'presenca_no_pipe', 'regime'
    ]

    query = f"SELECT {', '.join(fields)} FROM precatorios WHERE esta_na_ordem = TRUE LIMIT 5"
    print(f"Query: {query}\n")

    cursor.execute(query)
    results = cursor.fetchall()

    print(f"Registros retornados: {len(results)}\n")

    if results:
        print("Primeiro registro:")
        for key, value in results[0].items():
            print(f"  {key}: {value}")

    cursor.close()
    conn.close()

    print("\n[OK] Query funcionou!")

except psycopg2.Error as e:
    print(f"\n[ERRO] Query falhou: {e}")
    print(f"Codigo: {e.pgcode}")
except Exception as e:
    print(f"\n[ERRO] {e}")
