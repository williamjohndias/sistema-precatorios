#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema Web de Gerenciamento de Precatórios - Versão Vercel
Interface web para visualizar e editar dados como uma planilha Excel
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime, timezone, timedelta, date
import json
import os
import re
import time

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
from typing import Dict, List, Any, Optional

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
            base_query = f"SELECT {', '.join(fields)} FROM {TABLE_NAME}"
            # Contagem pode ser custosa; evitamos COUNT para não estourar timeout
            count_query = None
            
            # Adicionar filtros
            where_conditions = []
            params = []

            # Processar filtro esta_na_ordem primeiro (filtro padrão se não especificado)
            esta_na_ordem_filter = filters.get('esta_na_ordem', 'true').strip().lower() if filters else 'true'

            # Validar valor do filtro esta_na_ordem
            if esta_na_ordem_filter in ('true', '1', 'sim', 's', 'yes', 'y'):
                where_conditions.append("esta_na_ordem = TRUE")
            elif esta_na_ordem_filter in ('false', '0', 'não', 'nao', 'n', 'no'):
                where_conditions.append("esta_na_ordem = FALSE")
            elif esta_na_ordem_filter == '' or esta_na_ordem_filter == 'todos' or esta_na_ordem_filter == 'all':
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
                        # Para valor, usar comparação <= (menor ou igual) e converter para float
                        if field == 'valor':
                            try:
                                # Converter valor para float
                                # O valor já vem normalizado da rota principal, mas pode ser string numérica
                                if isinstance(value, str):
                                    # Se já é numérico (sem caracteres especiais), usar diretamente
                                    try:
                                        valor_float = float(value)
                                    except ValueError:
                                        # Tentar normalizar se houver caracteres especiais
                                        normalized_val = value.replace('R$', '').replace(' ', '').replace(',', '.')
                                        normalized_val = re.sub(r"[^0-9.]", "", normalized_val)
                                        if normalized_val:
                                            valor_float = float(normalized_val)
                                        else:
                                            logger.warning(f"Valor inválido para filtro de {field}: {value}")
                                            continue
                                else:
                                    valor_float = float(value)
                                
                                # Aplicar filtro apenas se valor for válido e > 0
                                if valor_float > 0:
                                    where_conditions.append(f"{field} <= %s")
                                    params.append(valor_float)
                                else:
                                    logger.warning(f"Valor deve ser maior que zero: {valor_float}")
                            except (ValueError, TypeError) as e:
                                # Se não conseguir converter, ignora o filtro
                                logger.warning(f"Valor inválido para filtro de {field}: {value} - Erro: {e}")
                                continue
                        # Para filtros de valor range (valor_min e valor_max)
                        elif field == 'valor_min':
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
                            # Converter string 'true'/'false' para boolean PostgreSQL
                            if isinstance(value, str):
                                bool_value = value.lower() in ('true', '1', 'sim', 's', 'yes', 'y')
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
                        # Para ano_orc, garantir que seja integer
                        elif field == 'ano_orc':
                            try:
                                ano_int = int(value) if value else None
                                if ano_int is not None:
                                    where_conditions.append(f"{field} = %s")
                                    params.append(ano_int)
                            except (ValueError, TypeError):
                                logger.warning(f"Valor inválido para filtro de {field}: {value}")
                                continue
                        # Para campos dropdown texto, usar comparação exata
                        elif field in ['organizacao', 'prioridade', 'tribunal', 'natureza', 'situacao', 'regime']:
                            where_conditions.append(f"{field} = %s")
                            params.append(value)
                        else:
                            # Para outros campos texto (como precatorio), usar ILIKE
                            where_conditions.append(f"{field} ILIKE %s")
                            params.append(f"%{value}%")
            
            if where_conditions:
                where_clause = " WHERE " + " AND ".join(where_conditions)
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
            self.cursor.execute(base_query, params)
            data = self.cursor.fetchall()
            query_time = time.time() - start_time
            logger.info(f"Query executada em {query_time:.2f}s, retornou {len(data)} registros")

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
                self.cursor.execute(f"SET statement_timeout TO {timeout}")
                # Desabilitar sequential scan para forçar uso de índices
                self.cursor.execute("SET enable_seqscan = off")
            except Exception:
                pass
            
            # Construir WHERE clause com filtros ativos (dinâmico)
            where_conditions = ["esta_na_ordem = TRUE", f"{field} IS NOT NULL"]
            params = []
            
            # Aplicar filtros ativos (exceto o próprio campo que estamos buscando)
            if active_filters:
                for filter_field, filter_value in active_filters.items():
                    # Não aplicar filtro no próprio campo
                    if filter_field != field and filter_value:
                        # Campos de texto: usar igualdade exata para filtros dinâmicos (mais preciso)
                        if filter_field in ['organizacao', 'precatorio', 'tribunal', 'natureza', 'situacao', 'regime', 'prioridade']:
                            where_conditions.append(f"{filter_field} = %s")
                            params.append(filter_value)
                        # Campos numéricos: igualdade exata
                        elif filter_field in ['ordem', 'ano_orc']:
                            try:
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
                # Para campos pequenos, usar GROUP BY (muito mais rápido que DISTINCT)
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
            # Paginação: 50 registros por página inicial (MÁXIMA performance)
            # Usuário pode aumentar se necessário via parâmetro per_page
            per_page = int(request.args.get('per_page', 50))
            if per_page < 1:
                per_page = 50
            # Limite máximo de 2000 por página para manter boa performance
            if per_page > 2000:
                per_page = 2000
        except (ValueError, TypeError):
            per_page = 50
        
        # Parâmetros de ordenação (padrão: ordenar pela coluna 'ordem')
        sort_field = request.args.get('sort', 'ordem')
        sort_order = request.args.get('order', 'asc')
        
        # Filtros
        filters = {}
        filter_fields = ['precatorio', 'ordem', 'organizacao', 'regime', 'tipo', 'tribunal', 
                         'situacao', 'ano_orc', 'presenca_no_pipe', 'esta_na_ordem']
        for field in filter_fields:
            value = request.args.get(f'filter_{field}', '').strip()
            if value:
                filters[field] = value
        
        # Buscar valor máximo com cache (atualizado a cada 5 minutos)
        # Com índice idx_precatorios_esta_ordem_valor, a query é rápida (~100-500ms)
        max_valor = get_cached_max_valor()

        # Filtro de valor: "até R$" - aplicar somente se > 0
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

        raw_valor = request.args.get('filter_valor', '').strip()
        normalized_valor = _normalize_currency_str(raw_valor)

        # Apenas aplica filtro se valor for > 0
        if normalized_valor is not None and normalized_valor > 0:
            filters['valor'] = str(normalized_valor)
            logger.info(f"Filtro de valor aplicado: <= {normalized_valor}")
        else:
            logger.info(f"Filtro de valor não aplicado - valor recebido: {raw_valor}, normalizado: {normalized_valor}")
        
        # Filtro padrão: mostrar apenas precatórios que estão na ordem
        if 'esta_na_ordem' not in filters:
            filters['esta_na_ordem'] = 'true'
        
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
            active_filters_for_dynamic['organizacao'] = filters['organizacao']
        
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
        
        # Obter dados paginados com ordenação (PRIORIDADE MÁXIMA)
        try:
            result = db_manager.get_precatorios_paginated(page=page, per_page=per_page, filters=filters, sort_field=sort_field, sort_order=sort_order)
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

        return render_template('index.html',
                             precatorios=result['data'],
                             pagination=result['pagination'],
                             filters=filters,
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
            return render_template('error.html')
        except:
            # Se até renderizar erro falhar, retorna resposta simples
            return f"<h1>Erro</h1><p>Ocorreu um erro: {str(e)[:200]}</p>", 500
    finally:
        try:
            db_manager.disconnect()
        except:
            pass  # Não deixar erro de desconexão quebrar a resposta

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
            filters['esta_na_ordem'] = 'true'
        
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

@app.route('/api/get_filter_options', methods=['GET'])
def get_filter_options():
    """API para carregar opções de filtro DINÂMICAS baseadas em filtros ativos"""
    try:
        if not db_manager.connect():
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
        values = db_manager.get_filter_values(field, use_cache=False, limit_count=limit_count, search_term=search_term, active_filters=active_filters if active_filters else None)
        
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
        db_manager.disconnect()

# Configuração específica para Vercel
if __name__ == "__main__":
    # Para desenvolvimento local
    app.run(debug=True, host='0.0.0.0', port=5000)
else:
    # Para produção no Vercel
    pass
