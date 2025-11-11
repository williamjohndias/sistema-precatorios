#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema Web de Gerenciamento de Precatórios - Versão Vercel
Interface web para visualizar e editar dados como uma planilha Excel
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, Response
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import ProgrammingError
import logging
from datetime import datetime, timezone, timedelta, date
import json
import os
import re
import time
import csv
from collections import defaultdict
import unicodedata
import math
from typing import Dict, List, Any, Optional

# Configurar logging otimizado para Vercel
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuração do Flask otimizada para Vercel
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'sua_chave_secreta_aqui')
ADMIN_TOKEN = os.environ.get('ADMIN_TOKEN', 'admin')

# Filtro customizado para formatação monetária brasileira
@app.template_filter('currency_br')
def currency_br_filter(value):
    """Formata número para moeda brasileira: R$ 1.234.567,89"""
    try:
        if value is None or value == '':
            return 'R$ 0,00'
        # Converter para float se for string
        if isinstance(value, str):
            value = float(value.replace(',', '.'))
        # Formatar com separadores brasileiros
        formatted = f"{float(value):,.2f}"
        # Trocar separadores: 1,234.56 -> 1.234,56
        formatted = formatted.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')
        return f"R$ {formatted}"
    except (ValueError, TypeError):
        return 'R$ 0,00'

# Versão para debug (aparecer no rodapé)
APP_VERSION = "v2.0-optimized-2025-10-30"

# Configuração do banco de dados usando variáveis de ambiente
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'bdunicoprecs.c50cwuocuwro.sa-east-1.rds.amazonaws.com'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', '$P^iFe27^YP5cpBU3J&tqa'),
    'database': os.environ.get('DB_NAME', 'OCSC')
}

TABLE_NAME = 'precatorios'

# Função para obter horário brasileiro
def get_brazil_time():
    """Retorna o horário atual do Brasil (UTC-3)"""
    brazil_tz = timezone(timedelta(hours=-3))
    return datetime.now(brazil_tz)

import copy

class DatabaseManager:
    """Gerenciador de conexão com banco de dados otimizado para Vercel"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Conecta ao banco de dados com timeout otimizado"""
        try:
            # Configurações otimizadas para Vercel
            conn_params = DB_CONFIG.copy()
            conn_params.update({
                'connect_timeout': 15,  # Timeout aumentado para conexão mais estável
                'application_name': 'precatorios_vercel',
                'keepalives_idle': 600,  # Aumentado para manter conexão
                'keepalives_interval': 30,  # Intervalo razoável
                'keepalives_count': 5,  # Mais tentativas antes de desistir
                # Timeouts otimizados para queries com paginação e índices
                'options': '-c statement_timeout=30000 -c idle_in_transaction_session_timeout=30000 -c lock_timeout=5000'
            })
            
            logger.info(f"Tentando conectar ao banco: {conn_params['host']}:{conn_params['port']}")
            self.connection = psycopg2.connect(**conn_params, cursor_factory=RealDictCursor)
            self.cursor = self.connection.cursor()
            # Evitar manter transações abertas e facilitar rollback automático após erros
            try:
                self.connection.autocommit = True
            except Exception:
                pass
            logger.info("Conexão com banco estabelecida")
            return True
        except psycopg2.OperationalError as e:
            logger.error(f"Erro operacional na conexão: {e}")
            logger.error(f"Configuração usada: host={conn_params.get('host')}, port={conn_params.get('port')}, user={conn_params.get('user')}, database={conn_params.get('database')}")
            return False
        except psycopg2.Error as e:
            logger.error(f"Erro PostgreSQL: {e}")
            logger.error(f"Configuração usada: host={conn_params.get('host')}, port={conn_params.get('port')}, user={conn_params.get('user')}, database={conn_params.get('database')}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado na conexão: {e}")
            logger.error(f"Configuração usada: host={conn_params.get('host')}, port={conn_params.get('port')}, user={conn_params.get('user')}, database={conn_params.get('database')}")
            return False

    def apply_optimization_indexes(self) -> Dict[str, Any]:
        """Cria índices recomendados e executa ANALYZE (idempotente)."""
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    return {'success': False, 'message': 'Falha ao conectar'}

            statements = [
                # Lista principal (filtro + ordenação)
                "CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_ordem  ON precatorios(esta_na_ordem, ordem)",
                # Filtro por valor com filtro padrão
                "CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_valor  ON precatorios(esta_na_ordem, valor)",
                # Dropdowns
                "CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_prioridade ON precatorios(esta_na_ordem, prioridade)",
                "CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_regime     ON precatorios(esta_na_ordem, regime)",
                "CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_tribunal   ON precatorios(esta_na_ordem, tribunal)",
                "CREATE INDEX IF NOT EXISTS idx_precatorios_esta_ordem_natureza   ON precatorios(esta_na_ordem, natureza)",
                # Atualizar estatísticas
                "ANALYZE precatorios"
            ]

            created = []
            for sql in statements:
                try:
                    self.cursor.execute(sql)
                    created.append(sql.split(' IF NOT EXISTS ')[-1].split(' ON ')[0] if 'CREATE INDEX' in sql else sql)
                except psycopg2.Error as e:
                    logger.warning(f"Falha ao executar: {sql} -> {e}")
                    # Continuar mesmo com falhas pontuais
                    try:
                        self.connection.rollback()
                    except Exception:
                        pass
            return {'success': True, 'created': created}
        except Exception as e:
            logger.error(f"Erro ao aplicar índices: {e}")
            return {'success': False, 'message': str(e)}
    
    def disconnect(self):
        """Desconecta do banco de dados"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Desconectado do banco")
        except Exception as e:
            logger.error(f"Erro ao desconectar: {e}")
    
    def get_precatorios_paginated(self, page: int = 1, per_page: int = 50, filters: Dict[str, str] = None, sort_field: str = 'ordem', sort_order: str = 'asc') -> Dict[str, Any]:
        """Obtém precatórios com paginação, filtros e ordenação - otimizado para Vercel"""
        try:
            # Timeout de 30 segundos é suficiente com paginação e índices
            try:
                self.cursor.execute("SET statement_timeout TO 30000")
            except Exception:
                pass
            # Campos específicos solicitados (ordenados conforme especificação)
            # Incluindo todos os campos do banco que são necessários
            fields = [
                'id', 'precatorio', 'ordem', 'organizacao', 'prioridade', 'tribunal', 
                'natureza', 'data_base', 'situacao', 'esta_na_ordem', 
                'nao_esta_na_ordem', 'ano_orc', 'valor', 'presenca_no_pipe', 'regime'
            ]

            # Validar campo de ordenação: permitir apenas colunas seguras/indexadas
            safe_sort_fields = {'ordem', 'ano_orc', 'valor'}
            if sort_field not in safe_sort_fields:
                sort_field = 'ordem'
            if sort_order.upper() not in ['ASC', 'DESC']:
                sort_order = 'ASC'
            
            # Construir query base
            # Por enquanto, não calcular acumulativo na query (será calculado depois se necessário)
            # Isso evita problemas de performance e sintaxe
            fields_str = ', '.join(fields)
            base_query = f"SELECT {fields_str} FROM {TABLE_NAME}"
            # Contagem pode ser custosa; evitamos COUNT para não estourar timeout
            count_query = None
            
            # Adicionar filtros
            where_conditions = []
            params = []

            # Processar filtro esta_na_ordem primeiro (filtro padrão se não especificado)
            esta_na_ordem_filter = filters.get('esta_na_ordem', 'SIM').strip().upper() if filters else 'SIM'

            # Validar valor do filtro esta_na_ordem
            if esta_na_ordem_filter in ('TRUE', '1', 'SIM', 'S', 'YES', 'Y'):
                where_conditions.append("esta_na_ordem = TRUE")
            elif esta_na_ordem_filter in ('FALSE', '0', 'NÃO', 'NAO', 'N', 'NO'):
                where_conditions.append("esta_na_ordem = FALSE")
            elif esta_na_ordem_filter == '' or esta_na_ordem_filter == 'TODOS' or esta_na_ordem_filter == 'ALL':
                # Não adiciona filtro (mostrar todos)
                pass
            else:
                # Valor inválido - aplicar filtro padrão
                where_conditions.append("esta_na_ordem = TRUE")

            if filters:
                for field, value in filters.items():
                    # Pular esta_na_ordem pois já foi processado
                    if field == 'esta_na_ordem':
                        continue

                    # Permitir filtros especiais que não são colunas diretas
                    is_special_range = field in ('valor_min', 'valor_max')
                    if value and (field in fields or is_special_range):
                        # Para filtros de valor range (valor_min e valor_max)
                        if field == 'valor_min':
                            try:
                                # Converter valor mínimo para float
                                normalized_val = value.replace('R$', '').replace(' ', '').replace(',', '.')
                                normalized_val = re.sub(r"[^0-9.]", "", normalized_val)
                                if normalized_val:
                                    valor_min_float = float(normalized_val)
                                    where_conditions.append(f"valor >= %s")
                                    params.append(valor_min_float)
                            except (ValueError, TypeError):
                                logger.warning(f"Valor mínimo inválido: {value}")
                                continue
                        elif field == 'valor_max':
                            try:
                                # Converter valor máximo para float
                                normalized_val = value.replace('R$', '').replace(' ', '').replace(',', '.')
                                normalized_val = re.sub(r"[^0-9.]", "", normalized_val)
                                if normalized_val:
                                    valor_max_float = float(normalized_val)
                                    where_conditions.append(f"valor <= %s")
                                    params.append(valor_max_float)
                            except (ValueError, TypeError):
                                logger.warning(f"Valor máximo inválido: {value}")
                                continue
                        # Para campos booleanos (exceto esta_na_ordem que já foi processado), converter string para boolean
                        elif field in ['nao_esta_na_ordem', 'presenca_no_pipe']:
                            # Converter string 'SIM'/'NAO' para boolean PostgreSQL
                            if isinstance(value, str):
                                value_upper = value.strip().upper()
                                bool_value = value_upper in ('TRUE', '1', 'SIM', 'S', 'YES', 'Y')
                            else:
                                bool_value = bool(value)
                            where_conditions.append(f"{field} = %s")
                            params.append(bool_value)
                        # Para ordem, garantir que seja integer (comparação exata ou ILIKE para busca parcial)
                        elif field == 'ordem':
                            try:
                                # Se o valor contém apenas dígitos, tratar como integer
                                digits_only = re.sub(r"[^0-9]", "", str(value))
                                if digits_only and digits_only == str(value).strip():
                                    ordem_int = int(digits_only)
                                    where_conditions.append(f"{field} = %s")
                                    params.append(ordem_int)
                                else:
                                    # Se contém outros caracteres, usar ILIKE para busca parcial (como texto)
                                    where_conditions.append(f"CAST({field} AS TEXT) ILIKE %s")
                                    params.append(f"%{value}%")
                            except (ValueError, TypeError):
                                logger.warning(f"Valor inválido para filtro de {field}: {value}")
                                continue
                        # Para ano_orc, garantir que seja integer (pode ser lista ou valor único)
                        elif field == 'ano_orc':
                            try:
                                # Se for lista (múltipla seleção)
                                if isinstance(value, list):
                                    anos_int = [int(v) for v in value if v]
                                    if len(anos_int) > 0:
                                        placeholders = ','.join(['%s'] * len(anos_int))
                                        where_conditions.append(f"{field} IN ({placeholders})")
                                        params.extend(anos_int)
                                else:
                                    # Valor único
                                    ano_int = int(value) if value else None
                                    if ano_int is not None:
                                        where_conditions.append(f"{field} = %s")
                                        params.append(ano_int)
                            except (ValueError, TypeError):
                                logger.warning(f"Valor inválido para filtro de {field}: {value}")
                                continue
                        # Para campos dropdown texto, usar comparação exata ou IN para múltiplos valores
                        elif field in ['organizacao', 'prioridade', 'tribunal', 'natureza', 'situacao', 'regime']:
                            # Se for lista (múltipla seleção), usar IN
                            if isinstance(value, list):
                                if len(value) > 0:
                                    placeholders = ','.join(['%s'] * len(value))
                                    where_conditions.append(f"{field} IN ({placeholders})")
                                    params.extend(value)
                            else:
                                # Valor único, usar =
                                where_conditions.append(f"{field} = %s")
                                params.append(value)
                        else:
                            # Para outros campos texto (como precatorio), usar ILIKE
                            where_conditions.append(f"{field} ILIKE %s")
                            params.append(f"%{value}%")
            
            if where_conditions:
                where_clause = " WHERE " + " AND ".join(where_conditions)
                # Aplicar WHERE na query externa (após o CTE)
                base_query += where_clause
                if count_query is not None:
                    count_query += where_clause
            
            # Adicionar ordenação apenas se campo for seguro
            # Usar índice composto quando possível para melhor performance
            if sort_field in safe_sort_fields:
                # Se ordenando por ordem e há filtro esta_na_ordem, o índice composto será usado
                base_query += f" ORDER BY {sort_field} {sort_order.upper()}"
            else:
                # Fallback: sempre ordenar por ordem se campo não for seguro
                base_query += " ORDER BY ordem ASC"
            
            # Adicionar paginação
            offset = (page - 1) * per_page
            base_query += f" LIMIT {per_page} OFFSET {offset}"
            
            # Executar query principal primeiro (para evitar timeouts em COUNT)
            # Usar EXPLAIN para debug se necessário
            logger.info(f"Executando query: {base_query[:200]}... com {len(params)} parâmetros")
            start_time = time.time()
            try:
                self.cursor.execute(base_query, params)
                data = self.cursor.fetchall()
                query_time = time.time() - start_time
                logger.info(f"Query executada em {query_time:.2f}s, retornou {len(data)} registros")
            except psycopg2.Error as e:
                logger.error(f"Erro ao executar query: {e}")
                logger.error(f"Query completa: {base_query}")
                raise

            # Determinar se precisamos fazer COUNT() real
            # Se houver filtros além do filtro padrão esta_na_ordem, fazer COUNT()
            # Caso contrário, usar valor fixo conhecido
            has_custom_filters = False
            if filters:
                # Verificar se há filtros além do esta_na_ordem padrão
                for key, value in filters.items():
                    if key != 'esta_na_ordem' and value:
                        has_custom_filters = True
                        break

            if has_custom_filters:
                # Fazer COUNT() para filtros personalizados
                try:
                    count_query = f"SELECT COUNT(*) FROM {TABLE_NAME}"
                    if where_conditions:
                        count_query += " WHERE " + " AND ".join(where_conditions)
                    self.cursor.execute(count_query, params)
                    count_result = self.cursor.fetchone()
                    total_count = count_result['count'] if isinstance(count_result, dict) else count_result[0]
                except Exception as e:
                    logger.warning(f"Erro ao contar registros filtrados: {e}")
                    # Fallback: estimar baseado nos resultados
                    total_count = len(data) if page == 1 else len(data) * page
            else:
                # Usar count fixo conhecido: 84,405 registros com esta_na_ordem=TRUE
                # Evita query COUNT() lenta que causa timeouts
                TOTAL_RECORDS_IN_DB = 84405
                total_count = TOTAL_RECORDS_IN_DB
            
            # Calcular paginação
            total_pages = (total_count + per_page - 1) // per_page
            
            pagination = {
                'page': page,
                'per_page': per_page,
                'total': total_count,
                'total_count': total_count,
                'total_pages': total_pages,
                'has_prev': page > 1,
                'has_next': page < total_pages,
                'prev_num': page - 1 if page > 1 else None,
                'next_num': page + 1 if page < total_pages else None
            }
            
            return {
                'data': [dict(row) for row in data],
                'pagination': pagination
            }
            
        except psycopg2.Error as e:
            logger.error(f"Erro ao buscar precatórios: {e}")
            # Garantir que a sessão não fique em estado aborted
            try:
                if self.connection:
                    self.connection.rollback()
            except Exception:
                pass
            return {
                'data': [], 
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': 0,
                    'total_count': 0,
                    'total_pages': 0,
                    'has_prev': False,
                    'has_next': False,
                    'prev_num': None,
                    'next_num': None
                }
            }
    
    def get_filter_values(self, field: str, use_cache: bool = True, limit_count: int = None, search_term: str = None, active_filters: Dict[str, str] = None) -> List[str]:
        """Obtém valores únicos para um campo específico - DINÂMICO baseado em filtros ativos"""
        global _filter_values_cache, _filter_cache_timestamp
        
        # Campos pequenos: carregar TODOS de uma vez (prioridade, tribunal, natureza, regime, situacao)
        small_fields = ['prioridade', 'tribunal', 'natureza', 'regime', 'situacao', 'ano_orc']
        is_small_field = field in small_fields
        
        # Se há filtros ativos, não usar cache (valores são dinâmicos)
        if active_filters:
            use_cache = False
        
        # Garantir que há conexão válida antes de usar
        if not self.connection or self.connection.closed:
            if not self.connect():
                logger.error(f"Falha ao conectar para buscar valores de {field}")
                return []
        
        # Verificar se cursor ainda está válido
        if not self.cursor or self.cursor.closed:
            try:
                self.cursor = self.connection.cursor()
            except Exception as e:
                logger.error(f"Erro ao recriar cursor para {field}: {e}")
                if not self.connect():
                    return []
        
        # Verificar cache primeiro (aumentado para 1 hora para melhor performance)
        if use_cache:
            now = datetime.now()
            cache_key = field
            if cache_key in _filter_values_cache and cache_key in _filter_cache_timestamp:
                cache_age = now - _filter_cache_timestamp[cache_key]
                if cache_age < timedelta(hours=1):  # Cache válido por 1 hora
                    cached_values = _filter_values_cache[cache_key]
                    # Se há termo de busca, filtrar do cache
                    if search_term:
                        search_lower = search_term.lower()
                        filtered = [v for v in cached_values if search_lower in v.lower()]
                        if limit_count:
                            filtered = filtered[:limit_count]
                        return filtered
                    # Se pediu um limite menor, retornar apenas os primeiros
                    if limit_count and limit_count < len(cached_values):
                        return cached_values[:limit_count]
                    return cached_values
        
        try:
            # Timeout ajustado por tipo de campo
            timeout = 15000 if field == 'organizacao' else 8000  # Mais tempo para organização
            
            # Para campos pequenos, SEM limite (carregar todos)
            # Para organização, SEM limite quando limit_count é None (carregar TODAS)
            if limit_count is None:
                if is_small_field:
                    limit_count = 1000  # Praticamente sem limite para campos pequenos
                else:
                    # Organização: sem limite quando None (carregar TODAS)
                    limit_count = None  # Manter None para carregar todas
            
            try:
                # Verificar se cursor ainda está válido antes de usar
                if not self.cursor or self.cursor.closed:
                    self.cursor = self.connection.cursor()
                self.cursor.execute(f"SET statement_timeout TO {timeout}")
                # Desabilitar sequential scan para forçar uso de índices
                self.cursor.execute("SET enable_seqscan = off")
            except Exception as e:
                logger.warning(f"Erro ao configurar timeout para {field}: {e}")
                # Tentar recriar cursor se necessário
                if not self.cursor or self.cursor.closed:
                    try:
                        self.cursor = self.connection.cursor()
                    except:
                        if not self.connect():
                            return []
            
            # Construir WHERE clause com filtros ativos (dinâmico)
            where_conditions = ["esta_na_ordem = TRUE", f"{field} IS NOT NULL"]
            params = []
            
            # Aplicar filtros ativos (exceto o próprio campo que estamos buscando)
            if active_filters:
                for filter_field, filter_value in active_filters.items():
                    # Não aplicar filtro no próprio campo
                    if filter_field != field and filter_value:
                        # Campos de texto: usar igualdade exata ou IN para múltiplos valores
                        if filter_field in ['organizacao', 'precatorio', 'tribunal', 'natureza', 'situacao', 'regime', 'prioridade']:
                            # Se for string com vírgulas (múltiplos valores), converter para lista
                            if isinstance(filter_value, str) and ',' in filter_value:
                                filter_values_list = [v.strip() for v in filter_value.split(',') if v.strip()]
                                if len(filter_values_list) > 1:
                                    placeholders = ','.join(['%s'] * len(filter_values_list))
                                    where_conditions.append(f"{filter_field} IN ({placeholders})")
                                    params.extend(filter_values_list)
                                elif len(filter_values_list) == 1:
                                    where_conditions.append(f"{filter_field} = %s")
                                    params.append(filter_values_list[0])
                            elif isinstance(filter_value, list):
                                if len(filter_value) > 1:
                                    placeholders = ','.join(['%s'] * len(filter_value))
                                    where_conditions.append(f"{filter_field} IN ({placeholders})")
                                    params.extend(filter_value)
                                elif len(filter_value) == 1:
                                    where_conditions.append(f"{filter_field} = %s")
                                    params.append(filter_value[0])
                            else:
                                where_conditions.append(f"{filter_field} = %s")
                                params.append(filter_value)
                        # Campos numéricos: igualdade exata ou IN
                        elif filter_field in ['ordem', 'ano_orc']:
                            try:
                                # Se for string com vírgulas (múltiplos valores)
                                if isinstance(filter_value, str) and ',' in filter_value:
                                    filter_values_list = [int(v.strip()) for v in filter_value.split(',') if v.strip()]
                                    if len(filter_values_list) > 1:
                                        placeholders = ','.join(['%s'] * len(filter_values_list))
                                        where_conditions.append(f"{filter_field} IN ({placeholders})")
                                        params.extend(filter_values_list)
                                    elif len(filter_values_list) == 1:
                                        where_conditions.append(f"{filter_field} = %s")
                                        params.append(filter_values_list[0])
                                elif isinstance(filter_value, list):
                                    filter_values_list = [int(v) for v in filter_value if v]
                                    if len(filter_values_list) > 1:
                                        placeholders = ','.join(['%s'] * len(filter_values_list))
                                        where_conditions.append(f"{filter_field} IN ({placeholders})")
                                        params.extend(filter_values_list)
                                    elif len(filter_values_list) == 1:
                                        where_conditions.append(f"{filter_field} = %s")
                                        params.append(filter_values_list[0])
                                else:
                                    where_conditions.append(f"{filter_field} = %s")
                                    params.append(int(filter_value))
                            except (ValueError, TypeError):
                                pass
                        # Campos booleanos
                        elif filter_field == 'esta_na_ordem':
                            where_conditions.append(f"{filter_field} = %s")
                            params.append(filter_value.lower() == 'true')
                        # Campo valor (já tratado separadamente)
                        elif filter_field == 'valor':
                            try:
                                where_conditions.append(f"{filter_field} <= %s")
                                params.append(float(filter_value))
                            except (ValueError, TypeError):
                                pass
            
            # Adicionar busca por termo se houver
            if search_term:
                search_pattern = f"%{search_term}%"
                where_conditions.append(f"{field} ILIKE %s")
                params.append(search_pattern)
            
            where_clause = " AND ".join(where_conditions)
            
            # ESTRATÉGIA OTIMIZADA: usar GROUP BY para campos pequenos (mais rápido)
            # Para organização, usar ORDER BY + LIMIT e filtrar únicos em Python
            if is_small_field:
                query = (
                    f"SELECT {field} "
                    f"FROM {TABLE_NAME} "
                    f"WHERE {where_clause} "
                    f"GROUP BY {field} "
                    f"ORDER BY {field}"
                )
                # Sem LIMIT para campos pequenos - queremos TODOS os valores dinâmicos
            else:
                # Para organização, usar GROUP BY quando não há limite (mais eficiente)
                if limit_count is None:
                    # Sem limite: usar GROUP BY para carregar TODAS as organizações
                    query = (
                        f"SELECT {field} "
                        f"FROM {TABLE_NAME} "
                        f"WHERE {where_clause} "
                        f"GROUP BY {field} "
                        f"ORDER BY {field}"
                    )
                else:
                    # Com limite: pegar mais linhas e filtrar únicos em Python
                    query = (
                        f"SELECT {field} "
                        f"FROM {TABLE_NAME} "
                        f"WHERE {where_clause} "
                        f"ORDER BY {field} "
                        f"LIMIT {limit_count * 3}"  # Pegar 3x mais para garantir únicos
                    )
            
            logger.info(f"Executando query DINÂMICA para {field} (limite: {limit_count}, busca: {search_term}, filtros ativos: {len(active_filters) if active_filters else 0}, campo pequeno: {is_small_field})...")
            start_time = time.time()
            
            # Verificar cursor antes de executar
            if not self.cursor or self.cursor.closed:
                if not self.connection or self.connection.closed:
                    if not self.connect():
                        return []
                else:
                    self.cursor = self.connection.cursor()
            
            self.cursor.execute(query, params)
            all_results = self.cursor.fetchall()
            query_time = time.time() - start_time
            
            # Para campos pequenos e organização sem limite, já vem único do GROUP BY
            # Para organização com limite, filtrar únicos em Python
            if is_small_field or (field == 'organizacao' and limit_count is None):
                values = [str(row[field]) for row in all_results if row[field] is not None]
            else:
                # Extrair valores únicos em Python
                seen = set()
                values = []
                for row in all_results:
                    value = str(row[field]) if row[field] is not None else None
                    if value and value not in seen:
                        seen.add(value)
                        values.append(value)
                        if limit_count and len(values) >= limit_count:
                            break
            
            logger.info(f"Query para {field} executada em {query_time:.2f}s, retornou {len(values)} valores")
            
            if len(values) == 0:
                logger.warning(f"Nenhum valor encontrado para {field}")
            
            # Atualizar cache (apenas se não houver busca)
            if use_cache and not search_term:
                _filter_values_cache[cache_key] = values
                _filter_cache_timestamp[cache_key] = datetime.now()
                logger.info(f"Cache atualizado para {field}: {len(values)} valores")
            
            return values
        except psycopg2.OperationalError as e:
            logger.error(f"Erro operacional ao buscar valores para {field}: {e}")
            # Retornar cache antigo se disponível
            if use_cache and cache_key in _filter_values_cache:
                logger.warning(f"Usando cache antigo para {field} devido a erro")
                return _filter_values_cache[cache_key]
            return []
        except psycopg2.Error as e:
            logger.error(f"Erro PostgreSQL ao buscar valores para {field}: {e}")
            # Retornar cache antigo se disponível
            if use_cache and cache_key in _filter_values_cache:
                logger.warning(f"Usando cache antigo para {field} devido a erro")
                return _filter_values_cache[cache_key]
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass
            return []
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar valores para {field}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Retornar cache antigo se disponível
            if use_cache and cache_key in _filter_values_cache:
                logger.warning(f"Usando cache antigo para {field} devido a erro")
                return _filter_values_cache[cache_key]
            return []

    def get_all_filter_values(self, fields: List[str]) -> Dict[str, List[str]]:
        """Obtém valores únicos para múltiplos campos otimizado"""
        try:
            results = {}
            
            # Executar queries de forma otimizada (limitar resultados e usar índices)
            for field in fields:
                try:
                    # Limitar a 50 resultados para melhorar performance e evitar timeout
                    query = f"SELECT DISTINCT {field} FROM {TABLE_NAME} WHERE {field} IS NOT NULL AND esta_na_ordem = TRUE ORDER BY {field} LIMIT 50"
                    self.cursor.execute(query)
                    field_results = self.cursor.fetchall()
                    results[field] = [str(row[field]) for row in field_results if row[field] is not None]
                except psycopg2.Error as e:
                    logger.warning(f"Erro ao buscar valores únicos para {field}: {e}")
                    results[field] = []
            
            return results
        except Exception as e:
            logger.error(f"Erro ao buscar valores únicos em batch: {e}")
            if self.connection:
                self.connection.rollback()
            return {field: [] for field in fields}
    
    def get_table_structure(self) -> Dict[str, Any]:
        """Retorna a estrutura da tabela precatorios para diagnóstico"""
        try:
            query = """
                SELECT column_name, data_type, is_nullable, character_maximum_length, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """
            self.cursor.execute(query, [TABLE_NAME])
            columns = self.cursor.fetchall()
            
            structure = {}
            for col in columns:
                structure[col['column_name']] = {
                    'data_type': col['data_type'],
                    'is_nullable': col['is_nullable'],
                    'max_length': col['character_maximum_length'],
                    'numeric_precision': col['numeric_precision'],
                    'numeric_scale': col['numeric_scale']
                }
            
            return structure
        except psycopg2.Error as e:
            logger.error(f"Erro ao obter estrutura da tabela: {e}")
            if self.connection:
                self.connection.rollback()
            return {}

    def get_quick_stats(self) -> Dict[str, Any]:
        """Métricas rápidas para validar comunicação e dados no banco."""
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    return {'ok': False, 'message': 'Falha ao conectar'}

            stats = {}
            # Total de linhas
            self.cursor.execute(f"SELECT COUNT(*) AS c FROM {TABLE_NAME}")
            stats['total'] = int(self.cursor.fetchone()['c'])

            # Apenas na ordem
            self.cursor.execute(f"SELECT COUNT(*) AS c FROM {TABLE_NAME} WHERE esta_na_ordem = TRUE")
            stats['total_na_ordem'] = int(self.cursor.fetchone()['c'])

            # Min/Max de valor considerando apenas registros válidos e na ordem
            self.cursor.execute(
                f"SELECT MIN(valor) AS min_valor, MAX(valor) AS max_valor FROM {TABLE_NAME} WHERE valor IS NOT NULL AND esta_na_ordem = TRUE"
            )
            row = self.cursor.fetchone()
            stats['min_valor'] = float(row['min_valor']) if row and row['min_valor'] is not None else None
            stats['max_valor'] = float(row['max_valor']) if row and row['max_valor'] is not None else None

            # Amostra de 5 registros (id, ordem, valor) na ordem
            self.cursor.execute(
                f"SELECT id, ordem, valor FROM {TABLE_NAME} WHERE esta_na_ordem = TRUE ORDER BY ordem LIMIT 5"
            )
            stats['sample'] = [dict(r) for r in self.cursor.fetchall()]

            return {'ok': True, 'stats': stats}
        except Exception as e:
            logger.error(f"Erro em quick stats: {e}")
            try:
                if self.connection:
                    self.connection.rollback()
            except Exception:
                pass
            return {'ok': False, 'message': str(e)}

    def get_max_value(self, field: str) -> float:
        """Obtém rapidamente o maior valor usando índice (ORDER BY DESC LIMIT 1)."""
        try:
            # Estratégia mais rápida que agregação MAX() em tabelas grandes
            query = f"""
                SELECT {field} AS max_valor
                FROM {TABLE_NAME}
                WHERE {field} IS NOT NULL AND esta_na_ordem = TRUE
                ORDER BY {field} DESC
                LIMIT 1
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return float(result['max_valor']) if result and result['max_valor'] is not None else 0.0
        except psycopg2.Error as e:
            logger.error(f"Erro ao buscar valor máximo para {field}: {e}")
            if self.connection:
                self.connection.rollback()
            # Fallback seguro para não travar a página
            return 0.0

    def get_log_filter_values(self, field: str) -> List[str]:
        """Obtém valores únicos para filtros de logs"""
        try:
            if not self.connection or self.connection.closed:
                if not self.connect():
                    return []
            
            # Mapear campos para colunas da tabela
            field_mapping = {
                'organizacao': 'l.organizacao',
                'prioridade': 'l.prioridade', 
                'tribunal': 'l.tribunal',
                'campo_modificado': 'l.campo_modificado',
                'precatorio': 'l.precatorio'
            }
            
            if field not in field_mapping:
                return []
            
            column = field_mapping[field]
            query = f"""
                SELECT DISTINCT {column} as value
                FROM precatorios_logs l
                WHERE {column} IS NOT NULL AND {column} != ''
                ORDER BY {column}
            """
            
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            values = []
            for row in results:
                if isinstance(row, dict):
                    value = row['value']
                else:
                    value = row[0]
                
                if value and str(value).strip():
                    values.append(str(value).strip())
            
            return sorted(values)
            
        except psycopg2.Error as e:
            logger.error(f"Erro ao buscar valores únicos para {field}: {e}")
            if self.connection:
                self.connection.rollback()
            return []
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar valores únicos para {field}: {e}")
            return []

    def get_logs_paginated(self, page: int = 1, per_page: int = 50, filters: Dict[str, str] = None) -> Dict[str, Any]:
        """Obtém logs de alterações com paginação e filtros"""
        try:
            logger.info(f"Buscando logs - Página: {page}, Por página: {per_page}, Filtros: {filters}")
            
            # Verificar se há conexão ativa
            if not self.connection or self.connection.closed:
                if not self.connect():
                    logger.error("Erro ao conectar ao banco para buscar logs")
                    return {'data': [], 'pagination': {'page': 1, 'per_page': per_page, 'total': 0, 'total_count': 0, 'total_pages': 0, 'has_prev': False, 'has_next': False, 'prev_num': None, 'next_num': None}}

            # Construir query base usando a estrutura real da tabela
            base_query = """
                SELECT l.id, l.organizacao, l.prioridade, l.tribunal,
                       l.campo_modificado, l.valor_anterior, l.valor_novo, l.data_modificacao,
                       l.precatorio, l.ordem
                FROM precatorios_logs l
            """
            count_query = "SELECT COUNT(*) FROM precatorios_logs l"

            # Adicionar filtros
            where_conditions = []
            params = []

            if filters:
                # Campos com correspondência exata (dropdowns)
                exact_match_fields = ['organizacao', 'prioridade', 'tribunal', 'campo_modificado', 'precatorio']
                for field in exact_match_fields:
                    if filters.get(field):
                        where_conditions.append(f"l.{field} = %s")
                        params.append(filters[field])

                # Campos com correspondência parcial (datas)
                if filters.get('data_inicio'):
                    where_conditions.append("DATE(l.data_modificacao) >= %s")
                    params.append(filters['data_inicio'])

                if filters.get('data_fim'):
                    where_conditions.append("DATE(l.data_modificacao) <= %s")
                    params.append(filters['data_fim'])

            if where_conditions:
                where_clause = " WHERE " + " AND ".join(where_conditions)
                base_query += where_clause
                count_query += where_clause

            # Adicionar ordenação (mais recentes primeiro)
            base_query += " ORDER BY l.data_modificacao DESC"

            # Adicionar paginação
            offset = (page - 1) * per_page
            base_query += f" LIMIT {per_page} OFFSET {offset}"

            # Executar contagem
            self.cursor.execute(count_query, params)
            count_result = self.cursor.fetchone()
            total_count = count_result['count'] if isinstance(count_result, dict) else count_result[0]

            # Executar query principal
            self.cursor.execute(base_query, params)
            data = self.cursor.fetchall()

            logger.info(f"Query executada com sucesso. Total no banco: {total_count}, Retornados: {len(data)}")

            # Calcular paginação
            total_pages = (total_count + per_page - 1) // per_page

            pagination = {
                'page': page,
                'per_page': per_page,
                'total': total_count,
                'total_count': total_count,
                'total_pages': total_pages,
                'has_prev': page > 1,
                'has_next': page < total_pages,
                'prev_num': page - 1 if page > 1 else None,
                'next_num': page + 1 if page < total_pages else None
            }

            return {
                'data': [dict(row) for row in data],
                'pagination': pagination
            }

        except psycopg2.Error as e:
            logger.error(f"Erro ao buscar logs: {e}")
            return {
                'data': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': 0,
                    'total_count': 0,
                    'total_pages': 0,
                    'has_prev': False,
                    'has_next': False,
                    'prev_num': None,
                    'next_num': None
                }
            }

    def log_precatorio_change(self, precatorio_id: str, field: str, old_value: Any, new_value: Any, 
                              organizacao: str, prioridade: str, tribunal: str, precatorio: str, ordem: int) -> bool:
        """Registra uma alteração na tabela de logs"""
        try:
            log_query = """
                INSERT INTO precatorios_logs 
                (organizacao, prioridade, tribunal, campo_modificado, valor_anterior, valor_novo, 
                 data_modificacao, precatorio, ordem)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            log_values = [
                organizacao,
                prioridade,
                tribunal,
                field,
                str(old_value) if old_value is not None else None,
                str(new_value) if new_value is not None else None,
                get_brazil_time().replace(tzinfo=None),
                precatorio,
                ordem
            ]
            
            self.cursor.execute(log_query, log_values)
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Erro ao registrar log: {e}")
            return False

    def bulk_update_precatorios(self, updates_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Atualização em massa usando uma única query SQL - muito mais rápida"""
        try:
            if not updates_data:
                return {'success_count': 0, 'error_count': 0}
            
            # Preparar dados para atualização em massa
            update_fields = []
            case_statements = {}
            ids = []
            
            # Obter campos que serão atualizados
            first_update = updates_data[0]
            for field, value in first_update['updates'].items():
                if field != 'id':
                    update_fields.append(field)
                    case_statements[field] = f"CASE id "
            
            # Construir CASE statements para cada campo
            for update_data in updates_data:
                precatorio_id = update_data['id']
                updates = update_data['updates']
                ids.append(str(precatorio_id))
                
                for field in update_fields:
                    value = updates.get(field)
                    if value is not None:
                        # Formatar valor conforme tipo de campo
                        if field in ('ordem', 'ano_orc'):
                            # Campos integer
                            try:
                                int_value = int(value) if value is not None else None
                                if int_value is not None:
                                    case_statements[field] += f"WHEN {precatorio_id} THEN {int_value} "
                            except (ValueError, TypeError):
                                # Se não conseguir converter, mantém valor atual (ELSE field)
                                pass
                        elif field == 'valor':
                            # Campo numeric
                            try:
                                if isinstance(value, str):
                                    normalized_val = value.replace('R$', '').replace(' ', '').replace(',', '.')
                                    normalized_val = re.sub(r"[^0-9.]", "", normalized_val)
                                    if normalized_val:
                                        float_value = float(normalized_val)
                                        case_statements[field] += f"WHEN {precatorio_id} THEN {float_value} "
                                else:
                                    float_value = float(value)
                                    case_statements[field] += f"WHEN {precatorio_id} THEN {float_value} "
                            except (ValueError, TypeError):
                                pass
                        elif field in ('esta_na_ordem', 'nao_esta_na_ordem', 'presenca_no_pipe'):
                            # Campos boolean - PostgreSQL usa TRUE/FALSE sem aspas
                            if isinstance(value, str):
                                bool_value = value.lower() in ('true', '1', 'sim', 's', 'yes', 'y')
                            else:
                                bool_value = bool(value)
                            bool_str = 'TRUE' if bool_value else 'FALSE'
                            case_statements[field] += f"WHEN {precatorio_id} THEN {bool_str} "
                        elif field == 'data_base' and isinstance(value, (date, datetime)):
                            # Campo data
                            if isinstance(value, datetime):
                                date_value = value.date()
                            else:
                                date_value = value
                            case_statements[field] += f"WHEN {precatorio_id} THEN '{date_value}'::date "
                        else:
                            # Campos texto - escapar aspas simples para SQL
                            escaped_value = str(value).replace("'", "''")
                            case_statements[field] += f"WHEN {precatorio_id} THEN '{escaped_value}' "
            
            # Finalizar CASE statements
            for field in update_fields:
                case_statements[field] += "ELSE " + field + " END"
            
            # Adicionar timestamp de atualização
            current_time = get_brazil_time().replace(tzinfo=None)
            case_statements['data_atualizacao'] = f"CASE id "
            for precatorio_id in ids:
                case_statements['data_atualizacao'] += f"WHEN {precatorio_id} THEN '{current_time}' "
            case_statements['data_atualizacao'] += "ELSE data_atualizacao END"
            
            # Construir query de atualização em massa
            set_clauses = []
            for field in update_fields + ['data_atualizacao']:
                set_clauses.append(f"{field} = {case_statements[field]}")
            
            ids_str = ','.join(ids)
            query = f"""
                UPDATE {TABLE_NAME}
                SET {', '.join(set_clauses)}
                WHERE id IN ({ids_str})
            """
            
            logger.info(f"Executando atualização em massa para {len(ids)} registros")
            self.cursor.execute(query)
            
            # Registrar logs para cada alteração
            for update_data in updates_data:
                precatorio_id = update_data['id']
                updates = update_data['updates']
                current_data = update_data.get('current_data', {})
                
                for field, new_value in updates.items():
                    if field != 'id':
                        old_value = current_data.get(field)
                        if old_value != new_value:
                            self.log_precatorio_change(
                                precatorio_id=str(precatorio_id),
                                field=field,
                                old_value=old_value,
                                new_value=new_value,
                                organizacao=current_data.get('organizacao', ''),
                                prioridade=current_data.get('prioridade', ''),
                                tribunal=current_data.get('tribunal', ''),
                                precatorio=current_data.get('precatorio', ''),
                                ordem=current_data.get('ordem', 0)
                            )
            
            self.connection.commit()
            
            logger.info(f"Atualização em massa concluída: {len(ids)} registros")
            return {'success_count': len(ids), 'error_count': 0}
            
        except psycopg2.Error as e:
            logger.error(f"Erro na atualização em massa: {e}")
            if self.connection:
                self.connection.rollback()
            return {'success_count': 0, 'error_count': len(updates_data)}

    def update_precatorio(self, precatorio_id: str, updates: Dict[str, Any],
                          usuario: str = 'Sistema Web', ip_address: str = None, user_agent: str = None) -> bool:
        """Atualiza um precatório específico - otimizado para Vercel"""
        try:
            # Primeiro, buscar dados atuais para comparação
            current_query = f"SELECT organizacao, prioridade, tribunal, precatorio, ordem, {', '.join(updates.keys())} FROM {TABLE_NAME} WHERE id = %s"
            self.cursor.execute(current_query, [precatorio_id])
            current_data = self.cursor.fetchone()
            
            if not current_data:
                logger.error(f"Precatório ID {precatorio_id} não encontrado")
                return False
            
            # Preparar campos e valores para atualização
            fields = []
            values = []

            for field, value in updates.items():
                if field != 'id':  # Não atualizar a chave primária
                    fields.append(f"{field} = %s")
                    values.append(value)

            if not fields:
                return False

            # Adicionar timestamp de atualização
            fields.append("data_atualizacao = %s")
            values.append(get_brazil_time().replace(tzinfo=None))

            # Adicionar ID do precatório para WHERE
            values.append(precatorio_id)

            query = f"""
                UPDATE {TABLE_NAME}
                SET {', '.join(fields)}
                WHERE id = %s
            """

            self.cursor.execute(query, values)
            
            # Registrar logs para cada campo alterado
            for field, new_value in updates.items():
                if field != 'id':
                    old_value = current_data.get(field)
                    if old_value != new_value:  # Só registrar se houve mudança
                        self.log_precatorio_change(
                            precatorio_id=precatorio_id,
                            field=field,
                            old_value=old_value,
                            new_value=new_value,
                            organizacao=current_data.get('organizacao', ''),
                            prioridade=current_data.get('prioridade', ''),
                            tribunal=current_data.get('tribunal', ''),
                            precatorio=current_data.get('precatorio', ''),
                            ordem=current_data.get('ordem', 0)
                        )
            
            self.connection.commit()

            logger.info(f"Precatório ID {precatorio_id} atualizado com sucesso")
            return True

        except psycopg2.Error as e:
            logger.error(f"Erro ao atualizar precatório {precatorio_id}: {e}")
            if self.connection:
                self.connection.rollback()
            return False

# Função para ler o CSV e criar dicionário de teto de repasse por município
def normalize_text(text: str) -> str:
    """Remove acentos, espaços e caracteres especiais para comparação."""
    if not text:
        return ''
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(char for char in text if not unicodedata.combining(char))
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '', text)
    return text


def calculate_caprec(meses: int) -> str:
    """
    Calcula a classificação CAPREC baseada no número de meses.
    
    Regras:
    - <= 7: A+
    - <= 13: A
    - <= 19: B+
    - <= 25: B
    - <= 31: C+
    - <= 37: C
    - <= 43: D+
    - <= 49: D
    - <= 55: E+
    - <= 60: E
    - > 60: F
    """
    if meses is None:
        return None
    
    try:
        meses_int = int(meses) if isinstance(meses, (int, float)) else int(float(str(meses)))
    except (ValueError, TypeError):
        return None
    
    if meses_int <= 7:
        return "A+"
    elif meses_int <= 13:
        return "A"
    elif meses_int <= 19:
        return "B+"
    elif meses_int <= 25:
        return "B"
    elif meses_int <= 31:
        return "C+"
    elif meses_int <= 37:
        return "C"
    elif meses_int <= 43:
        return "D+"
    elif meses_int <= 49:
        return "D"
    elif meses_int <= 55:
        return "E+"
    elif meses_int <= 60:
        return "E"
    else:  # meses_int > 60
        return "F"


def load_teto_repasse_from_csv(csv_path='cálculo.csv'):
    """
    Lê o CSV e retorna um dicionário: {municipio: teto_repasse/12}
    """
    teto_dict = {}
    try:
        # Tentar diferentes caminhos possíveis
        possible_paths = [
            csv_path,
            os.path.join(os.path.dirname(__file__), csv_path),
            os.path.join(os.getcwd(), csv_path),
        ]
        
        file_found = False
        actual_path = None
        
        for path in possible_paths:
            if os.path.exists(path):
                actual_path = path
                file_found = True
                break
        
        if not file_found:
            logger.error(f"Arquivo CSV não encontrado. Tentou: {possible_paths}")
            return teto_dict
        
        logger.info(f"Lendo CSV do caminho: {actual_path}")
        
        with open(actual_path, 'r', encoding='utf-8', newline='') as f:
            # Ler a primeira linha (cabeçalho)
            reader = csv.reader(f)
            try:
                headers = next(reader)
            except StopIteration:
                logger.error("CSV está vazio")
                return teto_dict
            
            # Encontrar os índices das colunas necessárias
            teto_col_idx = None
            ente_col_idx = None
            estado_col_idx = None
            
            for i, header in enumerate(headers):
                header_upper = header.upper()
                normalized_header = header_upper.replace('\n', ' ').strip()
                
                if estado_col_idx is None and 'ESTADO' in normalized_header:
                    estado_col_idx = i
                if ente_col_idx is None and ('ENTE DEVEDOR' in normalized_header or ('ENTE' in normalized_header and 'DEVEDOR' in normalized_header)):
                    ente_col_idx = i
                if teto_col_idx is None and 'TETO REPASSE PEC 66' in normalized_header:
                    teto_col_idx = i
                if estado_col_idx is not None and ente_col_idx is not None:
                    # continuar buscando até encontrar teto
                    pass
            
            if teto_col_idx is None:
                # Algumas planilhas podem trazer o texto com espaços extras
                for i, header in enumerate(headers):
                    if 'TETO REPASSE' in header.upper():
                        teto_col_idx = i
                        break

            if teto_col_idx is None or ente_col_idx is None:
                logger.error(f"Colunas não encontradas no CSV. Headers: {headers}")
                logger.error(f"teto_col_idx: {teto_col_idx}, ente_col_idx: {ente_col_idx}, estado_col_idx: {estado_col_idx}")
                return teto_dict
            
            # Log para debug
            logger.info(f"Índices encontrados - Ente Devedor: {ente_col_idx}, Estado: {estado_col_idx}, Teto: {teto_col_idx}")
            logger.info(f"Primeira linha (headers): {headers}")
            
            # Processar cada linha
            row_count = 0
            processed_count = 0
            required_indices = [idx for idx in [teto_col_idx, ente_col_idx, estado_col_idx] if idx is not None]
            max_required_idx = max(required_indices) if required_indices else 0
            
            for row in reader:
                row_count += 1
                if len(row) <= max_required_idx:
                    if row_count <= 5:
                        logger.warning(f"Linha {row_count} ignorada: muito curta (len={len(row)}, precisa >= {max_required_idx + 1})")
                    continue
                
                municipio = row[ente_col_idx].strip()
                teto_str = row[teto_col_idx].strip()
                
                if not municipio or not teto_str:
                    if row_count <= 5:
                        logger.warning(f"Linha {row_count} ignorada: municipio='{municipio}', teto_str='{teto_str}'")
                    continue
                
                processed_count += 1

                estado = ''
                if estado_col_idx is not None and len(row) > estado_col_idx:
                    estado = row[estado_col_idx].strip()
                
                # Normalizar valor monetário (remover R$, espaços, converter vírgula para ponto)
                try:
                    teto_clean = teto_str.replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.')
                    teto_value = float(teto_clean)
                    # Dividir por 12
                    teto_mensal = teto_value / 12
                    
                    # Armazenar por diferentes chaves para facilitar matching
                    # Prioridade: chaves com estado primeiro (mais específicas)
                    keys_to_store = []
                    
                    # 1. Chave principal: "Município - Estado" (se estado disponível)
                    if estado:
                        keys_to_store.append(f"{municipio} - {estado}")
                        keys_to_store.append(f"{municipio}/{estado}")
                    
                    # 2. Apenas município (fallback)
                    keys_to_store.append(municipio)
                    
                    # 3. Normalizações (sem acentos/pontuação) para todas as chaves
                    normalized_keys = []
                    for key in keys_to_store:
                        normalized = normalize_text(key)
                        if normalized and normalized not in normalized_keys:
                            normalized_keys.append(normalized)
                    
                    # Armazenar todas as chaves (com e sem normalização)
                    all_keys = keys_to_store + normalized_keys
                    for key in all_keys:
                        if key:  # Só armazenar se a chave não estiver vazia
                            teto_dict[key] = teto_mensal
                except (ValueError, TypeError) as e:
                    logger.warning(f"Erro ao processar teto para {municipio}: {teto_str} - {e}")
                    continue
        
        logger.info(f"CSV carregado: {len(teto_dict)} municípios processados (de {row_count} linhas, {processed_count} processadas)")
        if len(teto_dict) > 0:
            sample_items = list(teto_dict.items())[:5]
            logger.info(f"Exemplos de chaves no teto_dict (primeiras 5): {[f'{k}: {v:.2f}' for k, v in sample_items]}")
        else:
            logger.error("ATENÇÃO: teto_dict está vazio após processar CSV!")
            logger.error(f"Linhas lidas: {row_count}, Linhas processadas: {processed_count}")
        return teto_dict
    except FileNotFoundError:
        logger.error(f"Arquivo CSV não encontrado: {csv_path}")
        return {}
    except Exception as e:
        logger.error(f"Erro ao ler CSV: {e}")
        return {}

# Função para calcular acumulativo por município
def calculate_accumulative_by_municipio(db_manager, municipio=None):
    """
    Calcula o acumulativo (soma de valores ordenados por ordem) por município.
    Retorna um dicionário: {municipio: {'acumulativo': valor, 'count': quantidade}}
    """
    try:
        if not db_manager.connect():
            logger.error("Não foi possível conectar ao banco de dados")
            return {}
        
        # Query para calcular acumulativo por município
        # Ordena por ordem e soma os valores acumulativamente
        query = """
            WITH ordered_precatorios AS (
                SELECT 
                    organizacao,
                    ordem,
                    valor,
                    SUM(valor) OVER (
                        PARTITION BY organizacao 
                        ORDER BY ordem ASC 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) as acumulativo
                FROM precatorios
                WHERE esta_na_ordem = TRUE 
                    AND valor IS NOT NULL
                    AND organizacao IS NOT NULL
        """
        
        params = []
        if municipio:
            query += " AND organizacao = %s"
            params.append(municipio)
        
        query += """
            )
            SELECT 
                organizacao,
                MAX(acumulativo) as acumulativo_total,
                COUNT(*) as quantidade
            FROM ordered_precatorios
            GROUP BY organizacao
            ORDER BY organizacao
        """
        
        db_manager.cursor.execute(query, params)
        results = db_manager.cursor.fetchall()
        
        acumulativo_dict = {}
        for row in results:
            municipio_name = row['organizacao']
            acumulativo = float(row['acumulativo_total']) if row['acumulativo_total'] else 0.0
            quantidade = int(row['quantidade']) if row['quantidade'] else 0
            acumulativo_dict[municipio_name] = {
                'acumulativo': acumulativo,
                'quantidade': quantidade
            }
        
        logger.info(f"Cálculo de acumulativo concluído: {len(acumulativo_dict)} municípios")
        return acumulativo_dict
        
    except Exception as e:
        logger.error(f"Erro ao calcular acumulativo: {e}")
        return {}

# Cache para dicionário de tetos (carregado uma vez)
_cached_teto_dict = None
_teto_cache_timestamp = None

def get_teto_dict():
    """Retorna o dicionário de tetos com cache"""
    global _cached_teto_dict, _teto_cache_timestamp
    from datetime import datetime, timedelta
    
    now = datetime.now()
    cache_valid = (
        _cached_teto_dict is not None and
        _teto_cache_timestamp is not None and
        (now - _teto_cache_timestamp) < timedelta(hours=1)  # Cache de 1 hora
    )
    
    if not cache_valid:
        _cached_teto_dict = load_teto_repasse_from_csv()
        _teto_cache_timestamp = now
        logger.info(f"Teto dict recarregado: {len(_cached_teto_dict)} municípios")
    
    return _cached_teto_dict

def calculate_pec66_for_records(records):
    """
    Calcula o valor PEC 66 para cada registro na lista.
    Adiciona o campo 'pec66_resultado' e 'pec66_resultado_arredondado' a cada registro.
    """
    teto_dict = get_teto_dict()
    
    if not teto_dict:
        logger.warning("Teto dict vazio, não é possível calcular PEC 66")
        for record in records:
            record['pec66_resultado'] = None
            record['pec66_resultado_arredondado'] = None
        return records
    
    logger.info(f"Teto dict carregado com {len(teto_dict)} chaves. Primeiras 5 chaves: {list(teto_dict.keys())[:5]}")
    
    if len(records) > 0:
        logger.info(f"Processando {len(records)} registros. Primeiro registro: organizacao={records[0].get('organizacao')}, acumulativo_pec66={records[0].get('acumulativo_pec66')}")
    
    for idx, record in enumerate(records):
        organizacao = record.get('organizacao')
        acumulativo = record.get('acumulativo_pec66')
        
        if idx < 3:
            logger.info(f"Registro {idx}: organizacao='{organizacao}', acumulativo_pec66={acumulativo} (tipo: {type(acumulativo)})")
        
        if not organizacao:
            if idx < 3:
                logger.warning(f"Registro {idx}: organizacao está vazio")
            record['pec66_resultado'] = None
            record['pec66_resultado_arredondado'] = None
            continue
            
        # Converter acumulativo para float primeiro
        try:
            acumulativo_float = float(acumulativo) if acumulativo is not None else 0.0
        except (TypeError, ValueError):
            try:
                from decimal import Decimal
                acumulativo_float = float(Decimal(str(acumulativo))) if acumulativo is not None else 0.0
            except Exception:
                if idx < 3:
                    logger.warning(f"Registro {idx}: não foi possível converter acumulativo '{acumulativo}' para float")
                record['pec66_resultado'] = None
                record['pec66_resultado_arredondado'] = None
                continue
        
        if acumulativo is None or acumulativo_float == 0:
            if idx < 3:
                logger.info(f"Registro {idx}: acumulativo_pec66 está None ou 0 para organizacao '{organizacao}'")
            record['pec66_resultado'] = None
            record['pec66_resultado_arredondado'] = None
            continue
        
        # Extrair município e estado da organização (formato: "Município - UF" ou "Município/UF")
        municipio_org = organizacao.strip()
        estado_org = None
        
        # Tentar extrair estado do formato "Município - UF" ou "Município/UF"
        if ' - ' in municipio_org:
            parts = municipio_org.split(' - ', 1)
            municipio_org = parts[0].strip()
            estado_org = parts[1].strip() if len(parts) > 1 else None
        elif '/' in municipio_org:
            parts = municipio_org.split('/', 1)
            municipio_org = parts[0].strip()
            estado_org = parts[1].strip() if len(parts) > 1 else None
        
        # Tentar diferentes chaves de busca (prioridade: mais específico primeiro)
        teto_mensal = None
        search_keys = []
        
        # 1. Match exato com estado (se disponível)
        if estado_org:
            search_keys.append(f"{municipio_org} - {estado_org}")
            search_keys.append(f"{municipio_org}/{estado_org}")
        
        # 2. Match apenas com município
        search_keys.append(municipio_org)
        
        # 3. Versões normalizadas
        for key in search_keys:
            normalized = normalize_text(key)
            if normalized:
                search_keys.append(normalized)
        
        # 4. Organização completa original
        search_keys.append(organizacao)
        search_keys.append(normalize_text(organizacao))
        
        # Tentar encontrar nas chaves
        for key in search_keys:
            if key in teto_dict:
                teto_mensal = teto_dict[key]
                if idx < 3:  # Log apenas para os primeiros 3 registros
                    logger.info(f"Match encontrado para '{organizacao}': chave '{key}' -> teto_mensal={teto_mensal}")
                break
        
        # Se ainda não encontrou, tentar busca por nome similar (fallback)
        # Limitar busca para evitar travamento (máximo 100 iterações)
        if teto_mensal is None:
            municipio_lower = municipio_org.lower()
            estado_lower = estado_org.lower() if estado_org else None
            iteration_count = 0
            max_iterations = 100  # Limite para evitar travamento
            
            for csv_key, csv_teto in teto_dict.items():
                iteration_count += 1
                if iteration_count > max_iterations:
                    if idx < 3:
                        logger.warning(f"Busca por similaridade interrompida após {max_iterations} iterações para '{organizacao}'")
                    break
                
                csv_key_lower = csv_key.lower()
                # Match mais preciso: município deve estar contido na chave ou vice-versa
                if municipio_lower in csv_key_lower or csv_key_lower in municipio_lower:
                    # Se temos estado, verificar se o estado também corresponde
                    if estado_org and estado_lower:
                        if estado_lower in csv_key_lower:
                            teto_mensal = csv_teto
                            if idx < 3:
                                logger.info(f"Match por similaridade encontrado para '{organizacao}': chave '{csv_key}' -> teto_mensal={teto_mensal}")
                            break
                    else:
                        teto_mensal = csv_teto
                        if idx < 3:
                            logger.info(f"Match por similaridade encontrado para '{organizacao}': chave '{csv_key}' -> teto_mensal={teto_mensal}")
                        break
            
            # Se ainda não encontrou, logar para debug
            if teto_mensal is None and idx < 5:
                logger.warning(f"Não foi possível encontrar teto para '{organizacao}' (município: '{municipio_org}', estado: '{estado_org}')")
        
        if teto_mensal and teto_mensal > 0:
            # Calcular: acumulativo / teto_mensal
            # acumulativo_float já foi calculado acima
            try:
                resultado = acumulativo_float / teto_mensal
                resultado_arredondado = math.ceil(resultado)  # Arredondar para cima
                record['pec66_resultado'] = resultado
                record['pec66_resultado_arredondado'] = resultado_arredondado
                # Calcular CAPREC baseado nos meses arredondados
                record['caprec'] = calculate_caprec(resultado_arredondado)

                if idx < 5:
                    logger.info(
                        f"PEC66 calculado - Munic: {organizacao} | Acum: {acumulativo_float:.2f} | "
                        f"Teto/12: {teto_mensal:.2f} | Resultado: {resultado:.2f} | "
                        f"Arredondado: {resultado_arredondado} | CAPREC: {record['caprec']}"
                    )
            except Exception as e:
                logger.error(f"Erro ao calcular PEC66 para {organizacao}: {e}")
                record['pec66_resultado'] = None
                record['pec66_resultado_arredondado'] = None
                record['caprec'] = None
        else:
            if idx < 5:
                logger.warning(f"Não foi possível encontrar teto_mensal para '{organizacao}' (teto_mensal={teto_mensal})")
            record['pec66_resultado'] = None
            record['pec66_resultado_arredondado'] = None
            record['caprec'] = None

    return records


def enrich_records_with_pec66(records: List[Dict[str, Any]], db_manager: 'DatabaseManager') -> List[Dict[str, Any]]:
    """
    Popula os campos relacionados ao PEC 66 (acumulativo, meses e CAPREC).
    Usa abordagem simples: calcula acumulativo por organização individualmente.
    """
    if not records:
        return records

    # Inicializar campos
    for record in records:
        record['acumulativo_pec66'] = None
        record['pec66_resultado'] = None
        record['pec66_resultado_arredondado'] = None
        record['caprec'] = None

    # Agrupar registros por organização
    organizacoes_dict = {}
    for record in records:
        org = record.get('organizacao')
        if org:
            if org not in organizacoes_dict:
                organizacoes_dict[org] = []
            organizacoes_dict[org].append(record)

    if not organizacoes_dict:
        return calculate_pec66_for_records(records)

    try:
        # Garantir conexão válida
        if not db_manager:
            logger.error("db_manager não está disponível")
            return calculate_pec66_for_records(records)
        
        # Verificar se a conexão está aberta, mas não tentar reconectar se já estiver
        # A conexão deve ser gerenciada pela rota principal
        if not db_manager.connection:
            logger.error("Conexão não está disponível")
            return calculate_pec66_for_records(records)
        
        if db_manager.connection.closed:
            logger.warning("Conexão está fechada, tentando reconectar")
            if not db_manager.connect():
                logger.error("Não foi possível conectar ao banco de dados")
                return calculate_pec66_for_records(records)

        # Garantir que o cursor está disponível
        if not db_manager.cursor or db_manager.cursor.closed:
            try:
                if db_manager.connection and not db_manager.connection.closed:
                    db_manager.cursor = db_manager.connection.cursor()
                else:
                    logger.error("Não é possível criar cursor: conexão fechada")
                    return calculate_pec66_for_records(records)
            except Exception as e:
                logger.error(f"Erro ao criar cursor: {e}")
                return calculate_pec66_for_records(records)

        # OTIMIZAÇÃO: Buscar todos os acumulativos de uma vez usando uma única query
        # Descobrir a maior ordem necessária para cada organização
        max_ordem_por_org = {}
        for org, records_org in organizacoes_dict.items():
            max_ordem = 0
            for record in records_org:
                ordem = record.get('ordem')
                if ordem is not None:
                    try:
                        ordem_int = int(ordem)
                        max_ordem = max(max_ordem, ordem_int)
                    except (ValueError, TypeError):
                        pass
            if max_ordem > 0:
                max_ordem_por_org[org] = max_ordem

        if not max_ordem_por_org:
            # Se não há ordens válidas, retornar registros sem acumulativos
            logger.warning("Nenhuma ordem válida encontrada, retornando registros sem acumulativos")
            return calculate_pec66_for_records(records)
        
        try:
            # Verificar se cursor ainda está válido antes de usar
            if not db_manager.cursor or db_manager.cursor.closed:
                if db_manager.connection and not db_manager.connection.closed:
                    db_manager.cursor = db_manager.connection.cursor()
                else:
                    logger.warning("Conexão/cursor inválido, pulando cálculo de acumulativos mas retornando registros")
                    # Retornar registros mesmo sem acumulativos
                    return calculate_pec66_for_records(records)

            # Limitar número de organizações processadas para melhorar performance
            # Processar apenas as primeiras 15 organizações para não bloquear
            organizacoes_limitadas = dict(list(max_ordem_por_org.items())[:15])
            if len(max_ordem_por_org) > 15:
                logger.info(f"Limitando processamento a 15 organizações de {len(max_ordem_por_org)} totais para melhorar performance")
            
            # Construir query única com organizações limitadas usando IN
            organizacoes_list = list(organizacoes_limitadas.keys())
            placeholders = ','.join(['%s'] * len(organizacoes_list))
            
            # Query otimizada: busca registros das organizações limitadas de uma vez
            # Usa window function para calcular acumulativo diretamente no banco
            # LIMIT de 3000 registros para evitar queries muito lentas
            acum_query = f"""
                SELECT 
                    organizacao,
                    ordem,
                    SUM(COALESCE(valor, 0)) OVER (
                        PARTITION BY organizacao
                        ORDER BY ordem ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS acumulativo_pec66
                FROM {TABLE_NAME}
                WHERE organizacao IN ({placeholders})
                    AND esta_na_ordem = TRUE 
                    AND valor IS NOT NULL
                ORDER BY organizacao, ordem ASC
                LIMIT 3000
            """
            
            try:
                logger.info(f"Executando query de acumulativos para {len(organizacoes_list)} organizações")
                db_manager.cursor.execute(acum_query, organizacoes_list)
                all_rows = db_manager.cursor.fetchall()
                logger.info(f"Query retornou {len(all_rows)} linhas de acumulativos")
            except (ProgrammingError, Exception) as fetch_error:
                error_str = str(fetch_error).lower()
                if ('no results to fetch' in error_str or 
                    'no results' in error_str or
                    'no row' in error_str or
                    'result has already been consumed' in error_str):
                    all_rows = []
                else:
                    logger.warning(f"Erro ao buscar acumulativos: {fetch_error}")
                    all_rows = []

            # Organizar acumulativos por organização e ordem
            acumulativos_por_org: Dict[str, Dict[int, Optional[float]]] = defaultdict(dict)
            
            for row in all_rows:
                org = row.get('organizacao')
                ordem = row.get('ordem')
                acumulativo = row.get('acumulativo_pec66')
                if org is None or ordem is None:
                    continue
                try:
                    ordem_int = int(ordem)
                    acumulativo_float = float(acumulativo) if acumulativo is not None else None
                    acumulativos_por_org[org][ordem_int] = acumulativo_float
                except (ValueError, TypeError):
                    pass

            # Atribuir acumulativos aos registros da página atual
            atribuidos_count = 0
            for org, records_org in organizacoes_dict.items():
                org_acumulativos = acumulativos_por_org.get(org, {})
                for record in records_org:
                    ordem = record.get('ordem')
                    if ordem is not None:
                        try:
                            ordem_int = int(ordem)
                            acumulativo = org_acumulativos.get(ordem_int)
                            record['acumulativo_pec66'] = acumulativo
                            if acumulativo is not None:
                                atribuidos_count += 1
                        except (ValueError, TypeError):
                            pass
            
            logger.info(f"Atribuídos {atribuidos_count} acumulativos de {len(records)} registros. Organizações processadas: {len(acumulativos_por_org)}")

        except Exception as e:
            logger.error(f"Erro ao calcular acumulativos otimizado: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Fallback: tentar calcular individualmente apenas se a query otimizada falhar
            # Mas mesmo se falhar, retornar os registros sem acumulativos
            logger.warning("Usando fallback individual para cálculo de acumulativos")
            for org, records_org in organizacoes_dict.items():
                try:
                    org_query = f"""
                        SELECT ordem, valor
                        FROM {TABLE_NAME}
                        WHERE organizacao = %s 
                            AND esta_na_ordem = TRUE 
                            AND valor IS NOT NULL
                        ORDER BY ordem ASC
                        LIMIT 1000
                    """
                    if not db_manager.cursor or db_manager.cursor.closed:
                        if db_manager.connection and not db_manager.connection.closed:
                            db_manager.cursor = db_manager.connection.cursor()
                        else:
                            continue
                    
                    db_manager.cursor.execute(org_query, (org,))
                    org_rows = db_manager.cursor.fetchall()
                    
                    acumulativo = 0.0
                    acumulativo_dict = {}
                    for row in org_rows:
                        ordem = row.get('ordem')
                        valor = row.get('valor')
                        if ordem is not None and valor is not None:
                            try:
                                valor_float = float(valor) if isinstance(valor, (int, float)) else float(str(valor).replace(',', '.'))
                                acumulativo += valor_float
                                acumulativo_dict[int(ordem)] = acumulativo
                            except (ValueError, TypeError):
                                pass
                    
                    for record in records_org:
                        ordem = record.get('ordem')
                        if ordem is not None:
                            try:
                                ordem_int = int(ordem)
                                record['acumulativo_pec66'] = acumulativo_dict.get(ordem_int)
                            except (ValueError, TypeError):
                                pass
                except Exception as org_error:
                    logger.error(f"Erro ao calcular acumulativo individual para {org}: {org_error}")
                    continue

        # Calcular meses e CAPREC para todos os registros
        enriched = calculate_pec66_for_records(records)
        return enriched

    except Exception as e:
        logger.error(f"Erro ao enriquecer registros com PEC 66: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return calculate_pec66_for_records(records)

# Função principal para calcular os resultados
def calculate_pec66_results():
    """
    Calcula os resultados do PEC 66:
    1. Lê o CSV e obtém teto_repasse/12 por município
    2. Calcula acumulativo por município no banco
    3. Divide acumulativo por (teto_repasse/12) e arredonda
    Retorna lista de resultados
    """
    # Carregar teto de repasse do CSV
    teto_dict = load_teto_repasse_from_csv()
    
    if not teto_dict:
        logger.warning("Nenhum teto de repasse carregado do CSV")
        return []
    
    # Calcular acumulativo por município
    acumulativo_dict = calculate_accumulative_by_municipio(db_manager)
    
    if not acumulativo_dict:
        logger.warning("Nenhum acumulativo calculado")
        return []
    
    # Combinar dados e calcular resultado
    results = []
    for municipio, acum_data in acumulativo_dict.items():
        teto_mensal = teto_dict.get(municipio)
        
        if teto_mensal is None or teto_mensal == 0:
            # Se não encontrou no CSV, tentar buscar por nome similar
            # (alguns municípios podem ter nomes ligeiramente diferentes)
            for csv_municipio, csv_teto in teto_dict.items():
                if municipio.lower() in csv_municipio.lower() or csv_municipio.lower() in municipio.lower():
                    teto_mensal = csv_teto
                    break
        
        if teto_mensal and teto_mensal > 0:
            acumulativo = acum_data['acumulativo']
            # Calcular: acumulativo / (teto_repasse/12)
            resultado = acumulativo / teto_mensal
            resultado_arredondado = round(resultado)
            
            results.append({
                'municipio': municipio,
                'teto_repasse_anual': teto_mensal * 12,  # Para exibição
                'teto_repasse_mensal': teto_mensal,
                'acumulativo': acumulativo,
                'quantidade': acum_data['quantidade'],
                'resultado': resultado,
                'resultado_arredondado': resultado_arredondado
            })
        else:
            # Município sem teto no CSV
            results.append({
                'municipio': municipio,
                'teto_repasse_anual': None,
                'teto_repasse_mensal': None,
                'acumulativo': acum_data['acumulativo'],
                'quantidade': acum_data['quantidade'],
                'resultado': None,
                'resultado_arredondado': None
            })
    
    # Ordenar por resultado (maior primeiro)
    results.sort(key=lambda x: x['resultado'] if x['resultado'] is not None else -1, reverse=True)
    
    return results

# Instância global do gerenciador de banco
db_manager = DatabaseManager()

# Variáveis globais para controle de estado
original_data = {}
modified_data = {}

# Cache para valor máximo (atualizado a cada 5 minutos)
_cached_max_valor = None
_cache_timestamp = None

# Cache para valores de filtro (atualizado a cada 30 minutos para melhor performance)
_filter_values_cache = {}
_filter_cache_timestamp = {}

def get_cached_max_valor() -> float:
    """Retorna valor máximo com cache de 5 minutos para performance"""
    global _cached_max_valor, _cache_timestamp
    from datetime import datetime, timedelta

    now = datetime.now()
    cache_valid = (
        _cached_max_valor is not None and
        _cache_timestamp is not None and
        (now - _cache_timestamp) < timedelta(minutes=5)
    )

    if cache_valid:
        logger.info(f"Usando valor maximo em cache: {_cached_max_valor}")
        return _cached_max_valor

    # Cache expirado ou vazio - buscar do banco
    try:
        if db_manager.connect():
            # Com o índice idx_precatorios_esta_ordem_valor, esta query é RÁPIDA
            valor = db_manager.get_max_value('valor')
            if valor and valor > 0:
                _cached_max_valor = valor
                _cache_timestamp = now
                logger.info(f"Valor maximo atualizado no cache: {valor}")
                return valor
            db_manager.disconnect()
    except Exception as e:
        logger.warning(f"Erro ao buscar valor maximo, usando cache antigo ou padrao: {e}")

    # Fallback: retornar cache antigo ou valor padrão alto
    return _cached_max_valor if _cached_max_valor else 10000000.0

# ===== Normalização/Validação de tipos =====
def normalize_field_value(field_name: str, value: Any) -> Any:
    """Normaliza valores de campos para padrões consistentes.
    - ordem, ano_orc: inteiros extraindo somente dígitos
    - valor: string numérica padronizada com ponto decimal (ex.: 1234.56)
    - data_base: converte para formato de data
    - boolean fields: converte para boolean
    - demais: string aparada
    """
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()

    if field_name in ('ordem', 'ano_orc'):
        # Extrai apenas dígitos e converte para int, quando possível
        digits = re.sub(r"[^0-9]", "", str(value))
        if digits == '':
            return None
        try:
            return int(digits)
        except ValueError:
            return None

    if field_name == 'valor':
        # Remove símbolos e separadores de milhar e padroniza decimal com ponto
        s = str(value).strip()
        if not s:
            return None
        # troca vírgula decimal por ponto, remove espaços e R$
        s = s.replace('R$', '').replace(' ', '')
        # Se houver ambas vírgula e ponto, assume que o último separador é o decimal
        # Estratégia simples: remove todos os separadores exceto o último caractere [.,]
        # 1) substitui vírgula por ponto
        s = s.replace(',', '.')
        # 2) remove tudo que não seja dígito ou ponto
        s = re.sub(r"[^0-9.]", "", s)
        # 3) se houver múltiplos pontos, mantém somente o último como decimal
        if s.count('.') > 1:
            parts = s.split('.')
            decimal_part = parts.pop()
            s = ''.join(parts) + '.' + decimal_part
        # Converte para float para compatibilidade com tipo numeric do banco
        try:
            return float(s) if s else None
        except (ValueError, TypeError):
            return None

    if field_name == 'data_base':
        # Converte string para data se possível
        if value and isinstance(value, str):
            try:
                from datetime import datetime
                # Tenta diferentes formatos de data
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
            except:
                pass
        return value

    if field_name in ('esta_na_ordem', 'nao_esta_na_ordem', 'presenca_no_pipe'):
        # Converte para boolean
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'sim', 's', 'yes', 'y')
        return bool(value)

    # Campos de texto comuns
    return value

def normalize_updates(updates: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {}
    for k, v in updates.items():
        normalized[k] = normalize_field_value(k, v)
    return normalized

@app.route('/')
def index():
    """Página principal - otimizada para Vercel"""
    try:
        if not db_manager.connect():
            logger.error("Falha ao conectar com banco de dados")
            flash('Erro ao conectar com o banco de dados', 'error')
            return render_template('error.html')
        
        # Parâmetros de paginação com validação
        try:
            page = int(request.args.get('page', 1))
            if page < 1:
                page = 1
        except (ValueError, TypeError):
            page = 1
            
        try:
            # Paginação: 500 registros por página
            # Usuário pode aumentar se necessário via parâmetro per_page
            per_page = int(request.args.get('per_page', 500))
            if per_page < 1:
                per_page = 500
            # Limite máximo de 2000 por página para manter boa performance
            if per_page > 2000:
                per_page = 2000
        except (ValueError, TypeError):
            per_page = 500
        
        # Parâmetros de ordenação (padrão: ordenar pela coluna 'ordem')
        sort_field = request.args.get('sort', 'ordem')
        sort_order = request.args.get('order', 'asc')
        
        # Filtros
        filters = {}
        filter_fields = ['precatorio', 'ordem', 'organizacao', 'regime', 'tipo', 'tribunal', 
                         'situacao', 'ano_orc', 'presenca_no_pipe', 'esta_na_ordem']
        # Campos que suportam múltipla seleção
        multi_select_fields = ['prioridade', 'tribunal', 'natureza', 'situacao', 'ano_orc', 'regime']
        
        for field in filter_fields:
            # Para campos de múltipla seleção, usar getlist
            if field in multi_select_fields:
                values = request.args.getlist(f'filter_{field}')
                logger.info(f"Coletando filtro {field}: getlist() retornou {len(values)} valores: {values}")
                
                # Se getlist retornar apenas um valor e ele contiver vírgulas, dividir (fallback)
                if len(values) == 1 and ',' in values[0]:
                    values = [v.strip() for v in values[0].split(',') if v.strip()]
                    logger.info(f"Filtro {field}: Dividido por vírgula em {len(values)} valores: {values}")
                
                # Filtrar valores vazios e normalizar para string
                normalized_values = []
                for v in values:
                    if v is not None:
                        v_str = str(v).strip()
                        if v_str:  # Apenas adicionar se não for vazio após strip
                            normalized_values.append(v_str)
                
                if normalized_values:
                    filters[field] = normalized_values
                    logger.info(f"Filtro {field} final: {len(normalized_values)} valores -> {normalized_values}")
                else:
                    logger.info(f"Filtro {field}: Nenhum valor válido encontrado")
            else:
                value = request.args.get(f'filter_{field}', '').strip()
                if value:
                    filters[field] = value
        
        # Buscar valor máximo com cache (atualizado a cada 5 minutos)
        # Com índice idx_precatorios_esta_ordem_valor, a query é rápida (~100-500ms)
        max_valor = get_cached_max_valor()

        # Filtros de valor: valor mínimo e valor máximo
        def _normalize_currency_str(s: str):
            if not s or not s.strip():
                return None
            try:
                # Remover R$, espaços, e tratar separadores
                s = s.replace('R$', '').replace(' ', '').strip()
                
                # Se tem vírgula, assume que é separador decimal brasileiro
                # Remove pontos (separadores de milhar) e troca vírgula por ponto
                if ',' in s:
                    s = s.replace('.', '').replace(',', '.')
                # Remove tudo que não é número ou ponto decimal
                s = re.sub(r"[^0-9.]", "", s)
                
                return float(s) if s else None
            except Exception as e:
                logger.warning(f"Erro ao normalizar valor: {s} - {e}")
                return None

        # Processar valor mínimo
        raw_valor_min = request.args.get('filter_valor_min', '').strip()
        normalized_valor_min = _normalize_currency_str(raw_valor_min)
        if normalized_valor_min is not None and normalized_valor_min > 0:
            filters['valor_min'] = str(normalized_valor_min)
            logger.info(f"Filtro de valor mínimo aplicado: >= {normalized_valor_min}")

        # Processar valor máximo
        raw_valor_max = request.args.get('filter_valor_max', '').strip()
        normalized_valor_max = _normalize_currency_str(raw_valor_max)
        if normalized_valor_max is not None and normalized_valor_max > 0:
            filters['valor_max'] = str(normalized_valor_max)
            logger.info(f"Filtro de valor máximo aplicado: <= {normalized_valor_max}")
        
        # Filtro padrão: mostrar apenas precatórios que estão na ordem
        # Mas respeitar se o usuário selecionou "Todos" (valor vazio explícito)
        esta_na_ordem_param = request.args.get('filter_esta_na_ordem', None)
        if esta_na_ordem_param is not None:
            # O parâmetro foi enviado (usuário interagiu com o filtro)
            if esta_na_ordem_param.strip() == '':
                # Usuário selecionou "Todos" - não aplicar filtro
                pass  # Não adicionar ao filters, mostrar todos
            else:
                # Usuário selecionou "Sim" ou "Não" - já está em filters
                pass
        else:
            # Parâmetro não foi enviado (primeira carga ou não interagiu) - aplicar padrão
            filters['esta_na_ordem'] = 'SIM'
        
        # CARREGAR FILTROS: Organização carrega TODAS, outros filtros são dinâmicos baseados em filtros ativos
        logger.info("Carregando valores dos filtros no servidor...")
        
        filter_values = {
            'organizacao': [],
            'prioridade': [],
            'tribunal': [],
            'natureza': [],
            'situacao': [],
            'regime': [],
            'ano_orc': []
        }
        
        # ORGANIZAÇÃO: Carregar TODAS (sem limite) - sempre mostra todas as organizações do banco
        try:
            filter_values['organizacao'] = db_manager.get_filter_values('organizacao', use_cache=True, limit_count=None, active_filters=None)
            logger.info(f"Organização carregada: {len(filter_values['organizacao'])} valores (TODAS)")
        except Exception as e:
            logger.warning(f"Erro ao carregar organização: {e}")
            filter_values['organizacao'] = []
        
        # OUTROS FILTROS: Carregar baseado em filtros ativos (dinâmico)
        # Se houver filtro de organização aplicado, outros filtros mostram apenas valores dessa organização
        other_fields = ['prioridade', 'tribunal', 'natureza', 'situacao', 'regime', 'ano_orc']
        active_filters_for_dynamic = {}
        if filters.get('organizacao'):
            # Se organização for lista, pegar o primeiro valor para filtro dinâmico
            org_filter = filters['organizacao']
            if isinstance(org_filter, list) and len(org_filter) > 0:
                active_filters_for_dynamic['organizacao'] = org_filter[0]
            elif isinstance(org_filter, str):
                active_filters_for_dynamic['organizacao'] = org_filter
        
        for field in other_fields:
            try:
                # Se há filtro de organização, usar ele para filtrar dinamicamente
                # Caso contrário, carregar todos os valores
                filter_values[field] = db_manager.get_filter_values(
                    field, 
                    use_cache=not bool(active_filters_for_dynamic), 
                    limit_count=None,
                    active_filters=active_filters_for_dynamic if active_filters_for_dynamic else None
                )
                logger.info(f"Filtro {field} carregado: {len(filter_values[field])} valores (dinâmico: {bool(active_filters_for_dynamic)})")
            except Exception as e:
                logger.warning(f"Erro ao carregar {field}: {e}")
                filter_values[field] = []
        
        # Normalizar filtros ANTES de buscar dados (para garantir que a query SQL funcione corretamente)
        # Mas manter os filtros originais para o template também
        filters_for_query = {}
        for key, value in filters.items():
            if key in ['prioridade', 'tribunal', 'natureza', 'situacao', 'regime', 'ano_orc']:
                # Garantir que seja uma lista de strings para a query
                if isinstance(value, list):
                    normalized_list = []
                    for v in value:
                        if v is not None:
                            v_str = str(v).strip()
                            if v_str:
                                normalized_list.append(v_str)
                    if normalized_list:
                        filters_for_query[key] = normalized_list
                elif isinstance(value, str) and value:
                    filters_for_query[key] = [value.strip()]
            else:
                filters_for_query[key] = value
        
        # Obter dados paginados com ordenação (PRIORIDADE MÁXIMA)
        try:
            result = db_manager.get_precatorios_paginated(page=page, per_page=per_page, filters=filters_for_query, sort_field=sort_field, sort_order=sort_order)
            # Calcular acumulativo e PEC 66 (meses) para cada registro
            # OTIMIZAÇÃO: Fazer cálculo apenas se houver poucas organizações únicas (máx 15)
            # Para muitas organizações, inicializar campos como None e carregar página rapidamente
            registros_pagina = result.get('data', [])
            if registros_pagina:
                # Contar organizações únicas
                organizacoes_unicas = len({r.get('organizacao') for r in registros_pagina if r.get('organizacao')})
                
                # Inicializar campos como None primeiro (garantir que sempre existam)
                for record in registros_pagina:
                    record['acumulativo_pec66'] = None
                    record['pec66_resultado'] = None
                    record['pec66_resultado_arredondado'] = None
                    record['caprec'] = None
                
                # Apenas calcular se houver poucas organizações (para não bloquear carregamento)
                if organizacoes_unicas <= 15:
                    try:
                        # Tentar calcular PEC66 apenas se houver poucas organizações
                        enriched = enrich_records_with_pec66(registros_pagina, db_manager)
                        # Garantir que os dados enriquecidos sejam usados
                        if enriched:
                            result['data'] = enriched
                    except Exception as pec66_error:
                        logger.warning(f"Cálculo PEC66 falhou: {pec66_error}")
                        # Campos já foram inicializados como None acima
                else:
                    logger.info(f"Pulando cálculo PEC66 para melhorar performance: {organizacoes_unicas} organizações únicas na página")
                    # Campos já foram inicializados como None acima
        except Exception as e:
            logger.error(f"Erro ao buscar precatórios: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            result = {
                'data': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': 0,
                    'total_count': 0,
                    'total_pages': 0,
                    'has_prev': False,
                    'has_next': False,
                    'prev_num': None,
                    'next_num': None
                }
            }
        
        # max_valor já foi obtido anteriormente (não buscar novamente)
        
        # Campos específicos para exibição (ordenados conforme especificação)
        display_fields = [
            {'name': 'id', 'label': 'ID', 'type': 'integer', 'editable': False, 'visible': False},
            {'name': 'precatorio', 'label': 'Precatório', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'ordem', 'label': 'Ordem', 'type': 'integer', 'editable': False, 'visible': True},
            {'name': 'organizacao', 'label': 'Organização', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'prioridade', 'label': 'Prioridade', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'tribunal', 'label': 'Tribunal', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'natureza', 'label': 'Natureza', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'regime', 'label': 'Regime', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'ano_orc', 'label': 'Ano Orçamentário', 'type': 'integer', 'editable': False, 'visible': True},
            {'name': 'situacao', 'label': 'Situação', 'type': 'character varying', 'editable': True, 'visible': True},
            {'name': 'valor', 'label': 'Valor', 'type': 'numeric', 'editable': True, 'visible': True},
            {'name': 'acumulativo_pec66', 'label': 'Valor Acumulado', 'type': 'numeric', 'editable': False, 'visible': True},
            {'name': 'pec66_resultado_arredondado', 'label': 'Meses', 'type': 'numeric', 'editable': False, 'visible': True},
            {'name': 'caprec', 'label': 'CAPREC', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'presenca_no_pipe', 'label': 'No Pipe', 'type': 'boolean', 'editable': False, 'visible': True},
        ]
        
        # Armazenar dados originais para desfazer (simplificado - evitar deepcopy pesado)
        global original_data
        try:
            # Usar dict comprehension simples em vez de deepcopy para melhor performance
            original_data = {str(p['id']): dict(p) for p in result['data']}
        except Exception as e:
            logger.warning(f"Erro ao copiar dados originais: {e}")
            original_data = {}
        
        # Informações de ordenação
        sorting = {
            'field': sort_field,
            'order': sort_order
        }
        
        # Se não temos um máximo válido, não exibir no rótulo
        if max_valor == 0.0:
            max_valor = None

        # Normalizar filtros para garantir que campos multi-select sejam sempre listas de strings
        # IMPORTANTE: Manter todos os filtros, mesmo os vazios, para que o template possa gerar os hidden inputs corretos
        normalized_filters = {}
        for key, value in filters.items():
            if key in ['prioridade', 'tribunal', 'natureza', 'situacao', 'regime', 'ano_orc']:
                # Garantir que seja uma lista de strings
                if isinstance(value, list):
                    # Normalizar todos os valores para string
                    normalized_list = []
                    for v in value:
                        if v is not None:
                            v_str = str(v).strip()
                            if v_str:  # Apenas adicionar se não for vazio
                                normalized_list.append(v_str)
                    normalized_filters[key] = normalized_list
                    logger.info(f"Normalizando filtro {key}: lista com {len(value)} valores -> {len(normalized_list)} valores normalizados: {normalized_list}")
                elif isinstance(value, str) and value:
                    normalized_filters[key] = [value.strip()]
                    logger.info(f"Normalizando filtro {key}: string '{value}' -> lista com 1 valor")
                else:
                    # Se for None ou vazio, manter como lista vazia
                    normalized_filters[key] = []
            else:
                normalized_filters[key] = value
        
        # Log detalhado para debug
        logger.info(f"Filtros normalizados para template: {[(k, f'{len(v)} valores: {v}' if isinstance(v, list) else v) for k, v in normalized_filters.items() if k in ['prioridade', 'tribunal', 'natureza', 'situacao', 'regime', 'ano_orc']]}")

        return render_template('index.html',
                             precatorios=result['data'],
                             pagination=result['pagination'],
                             filters=normalized_filters,
                             filter_values=filter_values,
                             display_fields=display_fields,
                             sorting=sorting,
                             max_valor=max_valor)
    
    except Exception as e:
        logger.error(f"Erro crítico na página principal: {e}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")
        try:
            flash(f'Erro ao carregar dados: {str(e)[:100]}', 'error')
            try:
                return render_template('error.html')
            except:
                # Se template não existir, retornar HTML simples
                return f"""
                <!DOCTYPE html>
                <html>
                <head><title>Erro</title></head>
                <body>
                    <h1>Erro no Sistema</h1>
                    <p>Ocorreu um erro ao carregar os dados.</p>
                    <p>Erro: {str(e)[:200]}</p>
                    <p><a href="/">Tentar novamente</a></p>
                </body>
                </html>
                """, 500
        except Exception as render_error:
            # Se até renderizar erro falhar, retorna resposta simples
            logger.error(f"Erro ao renderizar página de erro: {render_error}")
            return f"<h1>Erro</h1><p>Ocorreu um erro: {str(e)[:200]}</p><p>Erro de renderização: {str(render_error)[:100]}</p>", 500
    finally:
        try:
            db_manager.disconnect()
        except:
            pass  # Não deixar erro de desconexão quebrar a resposta

# Handler de erro global para capturar erros não tratados
@app.errorhandler(404)
def not_found(error):
    return render_template('error.html'), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Erro interno do servidor: {error}")
    try:
        return render_template('error.html'), 500
    except:
        return "<h1>Erro Interno</h1><p>Ocorreu um erro no servidor.</p>", 500

@app.route('/update', methods=['POST'])
def update_data():
    """Atualiza os dados modificados no banco - otimizado para Vercel"""
    global modified_data
    
    try:
        logger.info("=== INÍCIO DA REQUISIÇÃO UPDATE ===")
        
        if not db_manager.connect():
            return jsonify({'success': False, 'message': 'Erro ao conectar com banco'})
        
        # Obter dados modificados do frontend
        modified_data = request.json.get('data', {})
        logger.info(f"Dados recebidos: {modified_data}")
        
        if not modified_data:
            logger.warning("Nenhum dado modificado recebido")
            return jsonify({'success': False, 'message': 'Nenhum dado para atualizar'})
        
        success_count = 0
        error_count = 0
        errors = []
        
        for precatorio_id, updates in modified_data.items():
            # Remover campos que não devem ser atualizados
            filtered_updates = {k: v for k, v in updates.items() 
                              if k != 'id' and v is not None}
            # Normalizar valores para padronização de tipos
            filtered_updates = normalize_updates(filtered_updates)
            
            if filtered_updates:
                # Obter informações do usuário para logging
                usuario = request.headers.get('User-Agent', 'Sistema Web')
                # Limitar tamanho do usuário para evitar erro de campo muito longo
                if len(usuario) > 100:
                    usuario = usuario[:97] + '...'
                ip_address = request.remote_addr
                
                if db_manager.update_precatorio(precatorio_id, filtered_updates, 
                                               usuario=usuario, ip_address=ip_address, 
                                               user_agent=request.headers.get('User-Agent')):
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(f"Erro ao atualizar precatório {precatorio_id}")
        
        # Limpar dados modificados após atualização
        modified_data.clear()
        
        if error_count == 0:
            message = f"Atualização concluída: {success_count} sucessos"
        else:
            message = f"Atualização concluída: {success_count} sucessos, {error_count} erros"
        
        return jsonify({
            'success': error_count == 0,
            'message': message,
            'success_count': success_count,
            'error_count': error_count,
            'errors': errors
        })
    
    except Exception as e:
        logger.error(f"Erro na atualização: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'success': False, 'message': f'Erro interno: {e}'})
    
    finally:
        db_manager.disconnect()

@app.route('/bulk_update', methods=['POST'])
def bulk_update():
    """Atualização em massa de registros selecionados - otimizado para Vercel"""
    try:
        logger.info("=== INÍCIO DA EDIÇÃO EM MASSA ===")
        
        if not db_manager.connect():
            return jsonify({'success': False, 'message': 'Erro ao conectar com banco'})
        
        data = request.json
        selected_ids = data.get('selected_ids', [])
        field_updates = data.get('field_updates', {})
        
        logger.info(f"IDs selecionados: {len(selected_ids)} registros")
        logger.info(f"Campos para atualizar: {field_updates}")
        
        if not selected_ids or not field_updates:
            return jsonify({'success': False, 'message': 'Nenhum registro ou campo selecionado'})
        
        # Normalizar valores para padronização de tipos
        normalized_updates = normalize_updates(field_updates)
        
        # Buscar dados atuais de todos os registros selecionados
        ids_str = ','.join(map(str, selected_ids))
        current_query = f"""
            SELECT id, organizacao, prioridade, tribunal, precatorio, ordem, 
                   {', '.join(normalized_updates.keys())}
            FROM {TABLE_NAME} 
            WHERE id IN ({ids_str})
        """
        
        db_manager.cursor.execute(current_query)
        current_data_list = db_manager.cursor.fetchall()
        
        # Preparar dados para atualização em massa
        updates_data = []
        for row in current_data_list:
            updates_data.append({
                'id': row['id'],
                'updates': normalized_updates,
                'current_data': dict(row)
            })
        
        # Executar atualização em massa
        result = db_manager.bulk_update_precatorios(updates_data)
        
        success_count = result['success_count']
        error_count = result['error_count']
        
        if error_count == 0:
            message = f"Atualização em massa concluída: {success_count} registros atualizados"
        else:
            message = f"Atualização em massa concluída: {success_count} sucessos, {error_count} erros"
        
        return jsonify({
            'success': error_count == 0,
            'message': message,
            'success_count': success_count,
            'error_count': error_count
        })
        
    except Exception as e:
        logger.error(f"Erro na atualização em massa: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'success': False, 'message': f'Erro interno: {e}'})
    
    finally:
        db_manager.disconnect()

@app.route('/api/get_all_ids', methods=['GET'])
def get_all_ids():
    """Retorna todos os IDs dos precatórios para seleção em massa"""
    try:
        if not db_manager.connect():
            return jsonify({'success': False, 'message': 'Erro ao conectar com banco'})
        
        # Buscar todos os IDs com filtros aplicados
        filters = {}
        for key, value in request.args.items():
            if key.startswith('filter_') and value:
                field_name = key.replace('filter_', '')
                filters[field_name] = value
        
        # Aplicar filtro padrão se não especificado
        if 'esta_na_ordem' not in filters:
            filters['esta_na_ordem'] = 'SIM'
        
        # Buscar IDs de forma mais eficiente usando query direta
        # Limitar a 5000 para evitar timeout (ajuste conforme necessário)
        try:
            where_conditions = []
            params = []
            
            # Construir filtros manualmente para query otimizada
            for key, value in filters.items():
                if value:
                    if key == 'esta_na_ordem':
                        where_conditions.append(f"{key} = %s")
                        params.append(True if str(value).lower() in ('true', '1') else False)
                    elif key == 'valor':
                        try:
                            normalized_val = str(value).replace('R$', '').replace(' ', '').replace(',', '.')
                            normalized_val = re.sub(r"[^0-9.]", "", normalized_val)
                            if normalized_val:
                                where_conditions.append(f"{key} <= %s")
                                params.append(float(normalized_val))
                        except (ValueError, TypeError):
                            pass
                    elif key in ['organizacao', 'prioridade', 'tribunal', 'natureza', 'situacao', 'regime']:
                        where_conditions.append(f"{key} = %s")
                        params.append(value)
                    elif key == 'ano_orc':
                        try:
                            where_conditions.append(f"{key} = %s")
                            params.append(int(value))
                        except (ValueError, TypeError):
                            pass
                    else:
                        # Para outros campos, usar ILIKE
                        where_conditions.append(f"{key} ILIKE %s")
                        params.append(f"%{value}%")
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            query = f"SELECT id FROM {TABLE_NAME}{where_clause} LIMIT 5000"
            
            if db_manager.cursor:
                db_manager.cursor.execute(query, params)
                results = db_manager.cursor.fetchall()
                ids = [str(row['id']) for row in results]
            else:
                raise Exception("Cursor não disponível")
        except Exception as e:
            logger.error(f"Erro ao buscar IDs: {e}")
            # Fallback para método anterior (limitado)
            result = db_manager.get_precatorios_paginated(page=1, per_page=5000, filters=filters)
            ids = [str(row['id']) for row in result['data']]
        
        return jsonify({
            'success': True,
            'ids': ids,
            'total': len(ids)
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar IDs: {e}")
        return jsonify({'success': False, 'message': str(e)})
    
    finally:
        db_manager.disconnect()

@app.route('/logs')
def logs():
    """Página de logs de alterações - otimizada para Vercel"""
    if not db_manager.connect():
        flash('Erro ao conectar com o banco de dados', 'error')
        return render_template('error.html')
    
    try:
        # Parâmetros de paginação
        try:
            page = int(request.args.get('page', 1))
            if page < 1:
                page = 1
        except (ValueError, TypeError):
            page = 1
            
        try:
            per_page = int(request.args.get('per_page', 50))
            if per_page < 1 or per_page > 1000:
                per_page = 50
        except (ValueError, TypeError):
            per_page = 50
        
        # Filtros
        filters = {}
        filter_fields = ['organizacao', 'prioridade', 'tribunal', 'campo_modificado', 'precatorio', 'data_inicio', 'data_fim']
        for field in filter_fields:
            value = request.args.get(f'filter_{field}', '').strip()
            if value:
                filters[field] = value

        # Obter valores únicos para os filtros dropdown
        filter_values = {}
        dropdown_fields = ['organizacao', 'prioridade', 'tribunal', 'campo_modificado', 'precatorio']
        for field in dropdown_fields:
            filter_values[field] = db_manager.get_log_filter_values(field)

        # Obter logs do banco de dados
        result = db_manager.get_logs_paginated(page=page, per_page=per_page, filters=filters)

        logger.info(f"Logs carregados: {len(result['data'])} registros")
        logger.info(f"Total de logs no banco: {result['pagination'].get('total_count', 0)}")

        return render_template('logs.html',
                             logs=result['data'],
                             pagination=result['pagination'],
                             filters=filters,
                             filter_values=filter_values)
    
    except Exception as e:
        logger.error(f"Erro na página de logs: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        flash(f'Erro ao carregar logs: {e}', 'error')
        return render_template('error.html')
    
    finally:
        db_manager.disconnect()

@app.route('/api/export_csv', methods=['GET'])
def export_csv():
    """Exporta os dados filtrados para CSV"""
    try:
        if not db_manager.connect():
            return jsonify({'success': False, 'message': 'Erro ao conectar com banco'}), 500
        
        # Processar filtros (mesma lógica da rota index)
        filters = {}
        filter_fields = ['precatorio', 'ordem', 'organizacao', 'regime', 'tipo', 'tribunal', 
                         'situacao', 'ano_orc', 'presenca_no_pipe', 'esta_na_ordem']
        # Campos que suportam múltipla seleção
        multi_select_fields = ['prioridade', 'tribunal', 'natureza', 'situacao', 'ano_orc', 'regime']
        
        for field in filter_fields:
            # Para campos de múltipla seleção, usar getlist
            if field in multi_select_fields:
                values = request.args.getlist(f'filter_{field}')
                # Se getlist retornar apenas um valor e ele contiver vírgulas, dividir (fallback)
                if len(values) == 1 and ',' in values[0]:
                    values = [v.strip() for v in values[0].split(',') if v.strip()]
                # Filtrar valores vazios
                values = [v.strip() for v in values if v and v.strip()]
                if values:
                    filters[field] = values
            else:
                value = request.args.get(f'filter_{field}', '').strip()
                if value:
                    filters[field] = value
        
        # Processar filtros de valor
        def _normalize_currency_str(s: str):
            if not s or not s.strip():
                return None
            try:
                s = s.replace('R$', '').replace(' ', '').strip()
                if ',' in s:
                    s = s.replace('.', '').replace(',', '.')
                s = re.sub(r"[^0-9.]", "", s)
                return float(s) if s else None
            except Exception as e:
                logger.warning(f"Erro ao normalizar valor: {s} - {e}")
                return None

        raw_valor_min = request.args.get('filter_valor_min', '').strip()
        normalized_valor_min = _normalize_currency_str(raw_valor_min)
        if normalized_valor_min is not None and normalized_valor_min > 0:
            filters['valor_min'] = str(normalized_valor_min)

        raw_valor_max = request.args.get('filter_valor_max', '').strip()
        normalized_valor_max = _normalize_currency_str(raw_valor_max)
        if normalized_valor_max is not None and normalized_valor_max > 0:
            filters['valor_max'] = str(normalized_valor_max)
        
        # Filtro padrão: esta_na_ordem
        esta_na_ordem_param = request.args.get('filter_esta_na_ordem', None)
        if esta_na_ordem_param is not None:
            if esta_na_ordem_param.strip() == '':
                pass  # Todos
            else:
                pass  # Já está em filters
        else:
            filters['esta_na_ordem'] = 'SIM'
        
        # Buscar TODOS os registros (sem paginação) com os filtros aplicados
        # Usar uma página grande para pegar todos (mas não infinito)
        result = db_manager.get_precatorios_paginated(page=1, per_page=50000, filters=filters, 
                                                      sort_field='ordem', sort_order='asc')
        
        records = result.get('data', [])
        
        if not records:
            return jsonify({'success': False, 'message': 'Nenhum registro encontrado para exportar'}), 404
        
        # Calcular PEC 66 e CAPREC para todos os registros (mesma lógica da rota index)
        if records:
            try:
                enrich_records_with_pec66(records, db_manager)
            except Exception as pec66_error:
                logger.error(f"Erro ao calcular PEC 66 para CSV: {pec66_error}")
                # Continuar mesmo com erro nos cálculos
        
        # Definir campos para exportação (campos visíveis)
        export_fields = [
            {'name': 'precatorio', 'label': 'Precatório'},
            {'name': 'ordem', 'label': 'Ordem'},
            {'name': 'organizacao', 'label': 'Organização'},
            {'name': 'prioridade', 'label': 'Prioridade'},
            {'name': 'tribunal', 'label': 'Tribunal'},
            {'name': 'natureza', 'label': 'Natureza'},
            {'name': 'regime', 'label': 'Regime'},
            {'name': 'ano_orc', 'label': 'Ano Orçamentário'},
            {'name': 'situacao', 'label': 'Situação'},
            {'name': 'valor', 'label': 'Valor'},
            {'name': 'acumulativo_pec66', 'label': 'Valor Acumulado'},
            {'name': 'pec66_resultado_arredondado', 'label': 'Meses'},
            {'name': 'caprec', 'label': 'CAPREC'},
            {'name': 'presenca_no_pipe', 'label': 'No Pipe'},
        ]
        
        # Criar CSV em memória
        import io
        output = io.StringIO()
        
        # Escrever cabeçalho
        headers = [field['label'] for field in export_fields]
        writer = csv.writer(output, delimiter=';', lineterminator='\n')
        writer.writerow(headers)
        
        # Escrever dados
        for record in records:
            row = []
            for field in export_fields:
                value = record.get(field['name'])
                
                # Formatar valores
                if value is None:
                    row.append('')
                elif field['name'] == 'valor' or field['name'] == 'acumulativo_pec66':
                    # Formatar como número brasileiro
                    try:
                        if isinstance(value, str):
                            value = float(value.replace(',', '.'))
                        formatted = f"{float(value):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                        row.append(formatted)
                    except:
                        row.append(str(value))
                elif field['name'] == 'presenca_no_pipe':
                    row.append('Sim' if value else 'Não')
                elif field['name'] == 'pec66_resultado_arredondado':
                    row.append(str(int(value)) if value is not None else '')
                else:
                    row.append(str(value))
            
            writer.writerow(row)
        
        # Preparar resposta
        output.seek(0)
        csv_data = output.getvalue()
        output.close()
        
        # Gerar nome do arquivo com timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'precatorios_{timestamp}.csv'
        
        # Retornar CSV
        response = Response(
            csv_data,
            mimetype='text/csv; charset=utf-8',
            headers={
                'Content-Disposition': f'attachment; filename={filename}',
                'Content-Type': 'text/csv; charset=utf-8'
            }
        )
        
        # Adicionar BOM para UTF-8 (para Excel abrir corretamente)
        response.data = '\ufeff' + csv_data
        
        return response
        
    except Exception as e:
        logger.error(f"Erro ao exportar CSV: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    
    finally:
        db_manager.disconnect()

@app.route('/undo', methods=['POST'])
def undo_changes():
    """Desfaz as alterações não salvas"""
    global original_data, modified_data
    
    try:
        # Limpar dados modificados
        modified_data.clear()
        
        return jsonify({
            'success': True,
            'message': 'Alterações desfeitas com sucesso'
        })
    
    except Exception as e:
        logger.error(f"Erro ao desfazer alterações: {e}")
        return jsonify({
            'success': False,
            'message': f'Erro ao desfazer alterações: {e}'
        })

@app.route('/refresh', methods=['POST'])
def refresh_data():
    """Recarrega os dados da página"""
    try:
        return jsonify({
            'success': True,
            'message': 'Dados recarregados com sucesso'
        })
    
    except Exception as e:
        logger.error(f"Erro ao recarregar dados: {e}")
        return jsonify({
            'success': False,
            'message': f'Erro ao recarregar dados: {e}'
        })

@app.route('/api/debug/structure', methods=['GET'])
def debug_table_structure():
    """Rota de diagnóstico para verificar estrutura da tabela"""
    try:
        if not db_manager.connect():
            return jsonify({'success': False, 'message': 'Erro ao conectar com banco'})
        
        structure = db_manager.get_table_structure()
        return jsonify({
            'success': True,
            'structure': structure,
            'columns': list(structure.keys()),
            'current_fields_in_code': [
                'id', 'precatorio', 'ordem', 'organizacao', 'prioridade', 'tribunal', 
                'natureza', 'data_base', 'situacao', 'esta_na_ordem', 
                'nao_esta_na_ordem', 'ano_orc', 'valor', 'presenca_no_pipe', 'regime'
            ]
        })
    except Exception as e:
        logger.error(f"Erro ao obter estrutura: {e}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        db_manager.disconnect()

@app.route('/api/debug/quick', methods=['GET'])
def debug_quick():
    """Endpoint de verificação rápida: total de linhas, min/max de valor e amostra."""
    try:
        if not db_manager.connect():
            return jsonify({'ok': False, 'message': 'Erro ao conectar com banco'}), 500
        result = db_manager.get_quick_stats()
        status = 200 if result.get('ok') else 500
        return jsonify(result), status
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500
    finally:
        db_manager.disconnect()

@app.route('/admin/apply_indexes', methods=['POST', 'GET'])
def admin_apply_indexes():
    """Aplica índices recomendados. Protegido por token simples via ENV ADMIN_TOKEN."""
    token = request.args.get('token') or request.headers.get('X-Admin-Token')
    if token != ADMIN_TOKEN:
        return jsonify({'success': False, 'message': 'Não autorizado'}), 401

    try:
        if not db_manager.connect():
            return jsonify({'success': False, 'message': 'Erro ao conectar com banco'}), 500
        result = db_manager.apply_optimization_indexes()
        return jsonify(result), (200 if result.get('success') else 500)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_manager.disconnect()

@app.route('/api/pec66_calculation', methods=['GET'])
def api_pec66_calculation():
    """API endpoint para retornar os cálculos do PEC 66"""
    try:
        results = calculate_pec66_results()
        return jsonify({
            'success': True,
            'data': results,
            'total': len(results)
        })
    except Exception as e:
        logger.error(f"Erro ao calcular PEC 66: {e}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/debug/pec66', methods=['GET'])
def debug_pec66():
    """Endpoint de debug para testar o cálculo PEC 66"""
    try:
        # Forçar reload do cache
        global _cached_teto_dict, _teto_cache_timestamp
        _cached_teto_dict = None
        _teto_cache_timestamp = None
        
        teto_dict = get_teto_dict()
        
        # Buscar alguns registros do banco
        if not db_manager.connect():
            return jsonify({'success': False, 'message': 'Erro ao conectar ao banco'}), 500
        
        query = """
            SELECT organizacao, ordem, valor,
                SUM(COALESCE(valor, 0)) OVER (
                    PARTITION BY organizacao 
                    ORDER BY ordem ASC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) as acumulativo_pec66
            FROM precatorios
            WHERE esta_na_ordem = TRUE AND valor IS NOT NULL
            ORDER BY organizacao, ordem
            LIMIT 10
        """
        
        db_manager.cursor.execute(query)
        records = db_manager.cursor.fetchall()
        
        # Calcular PEC 66 para esses registros
        test_records = [dict(r) for r in records]
        calculated = calculate_pec66_for_records(test_records)
        
        return jsonify({
            'success': True,
            'teto_dict_size': len(teto_dict),
            'teto_dict_sample': dict(list(teto_dict.items())[:5]),
            'records': calculated,
            'first_record_keys': list(calculated[0].keys()) if calculated else []
        })
    except Exception as e:
        import traceback
        logger.error(f"Erro no debug PEC 66: {e}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': str(e),
            'traceback': traceback.format_exc()
        }), 500
    finally:
        db_manager.disconnect()

@app.route('/pec66')
def pec66_page():
    """Página para exibir os cálculos do PEC 66"""
    try:
        results = calculate_pec66_results()
        return render_template('pec66.html', results=results)
    except Exception as e:
        logger.error(f"Erro ao carregar página PEC 66: {e}")
        flash(f'Erro ao carregar cálculos: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/api/get_filter_options', methods=['GET'])
def get_filter_options():
    """API para carregar opções de filtro DINÂMICAS baseadas em filtros ativos"""
    # Criar uma nova conexão para cada requisição (evita problemas com requisições paralelas)
    local_db = DatabaseManager()
    try:
        if not local_db.connect():
            return jsonify({'success': False, 'message': 'Erro ao conectar com banco'}), 500
        
        field = request.args.get('field', '')
        if field not in ['organizacao', 'prioridade', 'tribunal', 'natureza', 'situacao', 'regime', 'ano_orc']:
            return jsonify({'success': False, 'message': 'Campo inválido'}), 400
        
        # Permitir limite opcional e busca incremental
        limit = request.args.get('limit', None)
        limit_count = int(limit) if limit and limit.isdigit() else None
        
        search_term = request.args.get('search', '').strip()
        if not search_term:
            search_term = None
        
        # Obter filtros ativos da query string (para filtros dinâmicos)
        active_filters = {}
        filter_fields = ['organizacao', 'prioridade', 'tribunal', 'natureza', 'situacao', 'regime', 'ano_orc', 'ordem', 'precatorio', 'valor', 'esta_na_ordem']
        for filter_field in filter_fields:
            filter_value = request.args.get(f'active_filter_{filter_field}', '').strip()
            if filter_value:
                active_filters[filter_field] = filter_value
        
        # Usar estratégia dinâmica com filtros ativos
        values = local_db.get_filter_values(field, use_cache=False, limit_count=limit_count, search_term=search_term, active_filters=active_filters if active_filters else None)
        
        logger.info(f"API DINÂMICA: Retornando {len(values)} valores para {field} (filtros ativos: {len(active_filters)})")
        
        return jsonify({
            'success': True,
            'field': field,
            'values': values,
            'count': len(values),
            'has_more': limit_count is not None and len(values) == limit_count
        })
    except Exception as e:
        logger.error(f"Erro ao obter opções de filtro: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        local_db.disconnect()

# Configuração específica para Vercel
if __name__ == "__main__":
    # Para desenvolvimento local
    app.run(debug=True, host='0.0.0.0', port=5000)
else:
    # Para produção no Vercel
    pass
