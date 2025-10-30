#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script de diagnóstico da conexão com o banco de dados"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Configuração do banco
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'bdunicoprecs.c50cwuocuwro.sa-east-1.rds.amazonaws.com'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', '$P^iFe27^YP5cpBU3J&tqa'),
    'database': os.environ.get('DB_NAME', 'OCSC')
}

TABLE_NAME = 'precatorios'

print("=" * 80)
print("TESTE DE CONEXÃO COM O BANCO DE DADOS")
print("=" * 80)

try:
    # Testar conexão
    print("\n1. Tentando conectar ao banco...")
    print(f"   Host: {DB_CONFIG['host']}")
    print(f"   Port: {DB_CONFIG['port']}")
    print(f"   Database: {DB_CONFIG['database']}")
    print(f"   User: {DB_CONFIG['user']}")

    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor, connect_timeout=10)
    cursor = conn.cursor()
    print("   [OK] Conexao estabelecida com sucesso!")

    # Testar se a tabela existe
    print(f"\n2. Verificando se a tabela '{TABLE_NAME}' existe...")
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = %s
        )
    """, [TABLE_NAME])
    exists = cursor.fetchone()['exists']

    if exists:
        print(f"   [OK] Tabela '{TABLE_NAME}' encontrada!")
    else:
        print(f"   [ERRO] Tabela '{TABLE_NAME}' nao encontrada!")
        exit(1)

    # Contar total de registros
    print(f"\n3. Contando registros na tabela...")
    cursor.execute(f"SELECT COUNT(*) as total FROM {TABLE_NAME}")
    total = cursor.fetchone()['total']
    print(f"   [OK] Total de registros: {total}")

    # Contar registros com esta_na_ordem = TRUE
    print(f"\n4. Verificando filtro 'esta_na_ordem = TRUE'...")
    cursor.execute(f"SELECT COUNT(*) as total FROM {TABLE_NAME} WHERE esta_na_ordem = TRUE")
    total_na_ordem = cursor.fetchone()['total']
    print(f"   [OK] Registros com esta_na_ordem = TRUE: {total_na_ordem}")

    # Contar registros com esta_na_ordem = FALSE
    cursor.execute(f"SELECT COUNT(*) as total FROM {TABLE_NAME} WHERE esta_na_ordem = FALSE")
    total_fora_ordem = cursor.fetchone()['total']
    print(f"   [OK] Registros com esta_na_ordem = FALSE: {total_fora_ordem}")

    # Contar registros com esta_na_ordem = NULL
    cursor.execute(f"SELECT COUNT(*) as total FROM {TABLE_NAME} WHERE esta_na_ordem IS NULL")
    total_null = cursor.fetchone()['total']
    print(f"   [OK] Registros com esta_na_ordem = NULL: {total_null}")

    # Buscar primeiros 5 registros
    print(f"\n5. Buscando primeiros 5 registros (SEM filtro)...")
    cursor.execute(f"SELECT id, precatorio, ordem, esta_na_ordem FROM {TABLE_NAME} LIMIT 5")
    records = cursor.fetchall()
    for i, rec in enumerate(records, 1):
        print(f"   {i}. ID={rec['id']}, Precatório={rec['precatorio']}, Ordem={rec['ordem']}, Esta na Ordem={rec['esta_na_ordem']}")

    # Buscar primeiros 5 registros COM filtro
    print(f"\n6. Buscando primeiros 5 registros (COM filtro esta_na_ordem=TRUE)...")
    cursor.execute(f"SELECT id, precatorio, ordem, esta_na_ordem FROM {TABLE_NAME} WHERE esta_na_ordem = TRUE LIMIT 5")
    records_filtered = cursor.fetchall()
    if records_filtered:
        for i, rec in enumerate(records_filtered, 1):
            print(f"   {i}. ID={rec['id']}, Precatório={rec['precatorio']}, Ordem={rec['ordem']}, Esta na Ordem={rec['esta_na_ordem']}")
    else:
        print("   [PROBLEMA] Nenhum registro encontrado com esta_na_ordem = TRUE!")
        print("   [SOLUCAO] Remova o filtro padrao ou atualize os dados da coluna esta_na_ordem")

    # Verificar colunas da tabela
    print(f"\n7. Verificando estrutura da tabela...")
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, [TABLE_NAME])
    columns = cursor.fetchall()
    print(f"   [OK] Colunas encontradas: {len(columns)}")
    for col in columns[:15]:  # Primeiras 15 colunas
        print(f"      - {col['column_name']} ({col['data_type']})")

    cursor.close()
    conn.close()

    print("\n" + "=" * 80)
    print("DIAGNÓSTICO CONCLUÍDO COM SUCESSO")
    print("=" * 80)

except psycopg2.Error as e:
    print(f"\n[ERRO] ERRO DE BANCO DE DADOS: {e}")
    print(f"   Codigo: {e.pgcode}")
    print(f"   Detalhes: {e.pgerror}")
except Exception as e:
    print(f"\n[ERRO] {e}")
    import traceback
    traceback.print_exc()
