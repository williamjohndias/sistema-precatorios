#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Configuração do banco
DB_CONFIG = {
    'host': 'bdunicoprecs.c50cwuocuwro.sa-east-1.rds.amazonaws.com',
    'port': 5432,
    'user': 'postgres',
    'password': '$P^iFe27^YP5cpBU3J&tqa',
    'database': 'OCSC'
}

try:
    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    
    # Verificar estrutura da tabela
    cursor.execute("""
        SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'precatorios' 
        ORDER BY ordinal_position
    """)
    
    print('=== ESTRUTURA ATUAL DA TABELA PRECATORIOS ===')
    for row in cursor.fetchall():
        print(f'{row["column_name"]}: {row["data_type"]} (max_length: {row["character_maximum_length"]}, nullable: {row["is_nullable"]})')
    
    # Verificar alguns dados de exemplo
    cursor.execute('SELECT * FROM precatorios LIMIT 3')
    print('\n=== DADOS DE EXEMPLO ===')
    for row in cursor.fetchall():
        print(dict(row))
        
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f'Erro: {e}')
