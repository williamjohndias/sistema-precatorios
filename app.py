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
from datetime import datetime, timezone, timedelta
import json
import os
import re

# Configurar logging otimizado para Vercel
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuração do Flask otimizada para Vercel
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'sua_chave_secreta_aqui')

# Configuração do banco de dados usando variáveis de ambiente
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': os.environ.get('DB_NAME')
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
                'connect_timeout': 10,  # Timeout de conexão reduzido
                'application_name': 'precatorios_vercel',
                'keepalives_idle': 600,
                'keepalives_interval': 30,
                'keepalives_count': 3
            })
            
            self.connection = psycopg2.connect(**conn_params, cursor_factory=RealDictCursor)
            self.cursor = self.connection.cursor()
            logger.info("Conexão com banco estabelecida")
            return True
        except psycopg2.Error as e:
            logger.error(f"Erro ao conectar com banco: {e}")
            return False
    
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
            # Campos específicos solicitados (ordenados conforme especificação)
            fields = [
                'id', 'precatorio', 'ordem', 'organizacao', 'regime', 'ano_orc',
                'situacao', 'valor'
            ]

            # Validar campo de ordenação
            if sort_field not in fields:
                sort_field = 'ordem'
            
            if sort_order.upper() not in ['ASC', 'DESC']:
                sort_order = 'ASC'
            
            # Construir query base
            base_query = f"SELECT {', '.join(fields)} FROM {TABLE_NAME}"
            count_query = f"SELECT COUNT(*) FROM {TABLE_NAME}"
            
            # Adicionar filtros
            where_conditions = []
            params = []
            
            if filters:
                for field, value in filters.items():
                    if value and field in fields:
                        where_conditions.append(f"{field} ILIKE %s")
                        params.append(f"%{value}%")
            
            if where_conditions:
                where_clause = " WHERE " + " AND ".join(where_conditions)
                base_query += where_clause
                count_query += where_clause
            
            # Adicionar ordenação
            # Quando o campo for 'ordem' (armazenado como texto na base), ordenamos numericamentre
            if sort_field == 'ordem':
                # Remove caracteres não numéricos e faz cast para inteiro; valores não numéricos vão para o fim
                numeric_expr = (
                    "CASE WHEN NULLIF(regexp_replace(ordem, '[^0-9]', '', 'g'), '') IS NULL "
                    "THEN NULL ELSE NULLIF(regexp_replace(ordem, '[^0-9]', '', 'g'), '')::int END"
                )
                base_query += f" ORDER BY {numeric_expr} {sort_order.upper()}, id ASC"
            else:
                base_query += f" ORDER BY {sort_field} {sort_order.upper()}"
            
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
            return {'data': [], 'pagination': {}}
    
    def get_logs_paginated(self, page: int = 1, per_page: int = 50, filters: Dict[str, str] = None) -> Dict[str, Any]:
        """Obtém logs de alterações com paginação e filtros"""
        try:
            logger.info(f"Buscando logs - Página: {page}, Por página: {per_page}, Filtros: {filters}")

            # Construir query base com JOIN para pegar dados do precatório
            base_query = """
                SELECT l.id, l.data_alteracao, l.usuario, p.precatorio, p.organizacao,
                       l.campo_alterado, l.valor_anterior, l.valor_novo, l.ip_address
                FROM precatorios_logs l
                LEFT JOIN precatorios p ON l.precatorio_id = p.id
            """
            count_query = "SELECT COUNT(*) FROM precatorios_logs l"

            # Adicionar filtros
            where_conditions = []
            params = []

            if filters:
                if filters.get('usuario'):
                    where_conditions.append("l.usuario ILIKE %s")
                    params.append(f"%{filters['usuario']}%")

                if filters.get('campo'):
                    where_conditions.append("l.campo_alterado ILIKE %s")
                    params.append(f"%{filters['campo']}%")

                if filters.get('precatorio'):
                    where_conditions.append("p.precatorio ILIKE %s")
                    params.append(f"%{filters['precatorio']}%")

                if filters.get('data_inicio'):
                    where_conditions.append("DATE(l.data_alteracao) >= %s")
                    params.append(filters['data_inicio'])

                if filters.get('data_fim'):
                    where_conditions.append("DATE(l.data_alteracao) <= %s")
                    params.append(filters['data_fim'])

            if where_conditions:
                where_clause = " WHERE " + " AND ".join(where_conditions)
                base_query += where_clause
                count_query += where_clause

            # Adicionar ordenação (mais recentes primeiro)
            base_query += " ORDER BY l.data_alteracao DESC"

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

    def update_precatorio(self, precatorio_id: str, updates: Dict[str, Any],
                          usuario: str = 'Sistema Web', ip_address: str = None, user_agent: str = None) -> bool:
        """Atualiza um precatório específico - otimizado para Vercel"""
        try:
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

# ===== Normalização/Validação de tipos =====
def normalize_field_value(field_name: str, value: Any) -> Any:
    """Normaliza valores de campos para padrões consistentes.
    - ordem, ano_orc: inteiros extraindo somente dígitos
    - valor: string numérica padronizada com ponto decimal (ex.: 1234.56)
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
        # Mantém como string padronizada
        return s

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
            per_page = int(request.args.get('per_page', 1000))
            if per_page < 1 or per_page > 1000:
                per_page = 1000
        except (ValueError, TypeError):
            per_page = 1000
        
        # Parâmetros de ordenação (padrão: ordenar pela coluna 'ordem')
        sort_field = request.args.get('sort', 'ordem')
        sort_order = request.args.get('order', 'asc')
        
        # Filtros
        filters = {}
        filter_fields = ['precatorio', 'ordem', 'organizacao', 'regime', 'ano_orc', 'situacao', 'valor']
        for field in filter_fields:
            value = request.args.get(f'filter_{field}', '').strip()
            if value:
                filters[field] = value
        
        # Obter dados paginados com ordenação
        result = db_manager.get_precatorios_paginated(page=page, per_page=per_page, filters=filters, sort_field=sort_field, sort_order=sort_order)
        
        # Campos específicos para exibição (ordenados conforme especificação)
        display_fields = [
            {'name': 'id', 'label': 'ID', 'type': 'integer', 'editable': False, 'visible': False},
            {'name': 'precatorio', 'label': 'Precatório', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'ordem', 'label': 'Ordem', 'type': 'integer', 'editable': False, 'visible': True},
            {'name': 'organizacao', 'label': 'Organização', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'regime', 'label': 'Regime', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'ano_orc', 'label': 'Ano Orçamento', 'type': 'character varying', 'editable': False, 'visible': True},
            {'name': 'situacao', 'label': 'Situação', 'type': 'character varying', 'editable': True, 'visible': True},
            {'name': 'valor', 'label': 'Valor', 'type': 'character varying', 'editable': True, 'visible': True},
        ]
        
        # Armazenar dados originais para desfazer
        global original_data
        original_data = {str(p['id']): copy.deepcopy(p) for p in result['data']}
        
        # Informações de ordenação
        sorting = {
            'field': sort_field,
            'order': sort_order
        }
        
        return render_template('index.html',
                             precatorios=result['data'],
                             pagination=result['pagination'],
                             filters=filters,
                             display_fields=display_fields,
                             sorting=sorting)
    
    except Exception as e:
        logger.error(f"Erro na página principal: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        flash(f'Erro ao carregar dados: {e}', 'error')
        return render_template('error.html')
    
    finally:
        db_manager.disconnect()

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
        
        logger.info(f"IDs selecionados: {selected_ids}")
        logger.info(f"Campos para atualizar: {field_updates}")
        
        if not selected_ids or not field_updates:
            return jsonify({'success': False, 'message': 'Nenhum registro ou campo selecionado'})
        
        success_count = 0
        error_count = 0
        errors = []
        
        # Obter informações do usuário para logging
        usuario = request.headers.get('User-Agent', 'Sistema Web')
        if len(usuario) > 100:
            usuario = usuario[:97] + '...'
        ip_address = request.remote_addr
        
        for precatorio_id in selected_ids:
            try:
                # Normalizar valores para padronização de tipos
                normalized = normalize_updates(field_updates)
                if db_manager.update_precatorio(str(precatorio_id), normalized, 
                                              usuario=usuario, ip_address=ip_address, 
                                              user_agent=request.headers.get('User-Agent')):
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(f"Erro ao atualizar precatório {precatorio_id}")
            except Exception as e:
                error_count += 1
                errors.append(f"Erro ao atualizar precatório {precatorio_id}: {e}")
                logger.error(f"Erro na atualização em massa do precatório {precatorio_id}: {e}")
        
        message = f"Atualização em massa concluída: {success_count} sucessos, {error_count} erros"
        
        return jsonify({
            'success': error_count == 0,
            'message': message,
            'success_count': success_count,
            'error_count': error_count,
            'errors': errors
        })
        
    except Exception as e:
        logger.error(f"Erro na atualização em massa: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'success': False, 'message': f'Erro interno: {e}'})
    
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
        filter_fields = ['usuario', 'campo', 'precatorio', 'data_inicio', 'data_fim']
        for field in filter_fields:
            value = request.args.get(f'filter_{field}', '').strip()
            if value:
                filters[field] = value

        # Obter logs do banco de dados
        result = db_manager.get_logs_paginated(page=page, per_page=per_page, filters=filters)

        logger.info(f"Logs carregados: {len(result['data'])} registros")
        logger.info(f"Total de logs no banco: {result['pagination'].get('total_count', 0)}")

        return render_template('logs.html',
                             logs=result['data'],
                             pagination=result['pagination'],
                             filters=filters)
    
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

# Configuração específica para Vercel
if __name__ == "__main__":
    # Para desenvolvimento local
    app.run(debug=True, host='0.0.0.0', port=5000)
else:
    # Para produção no Vercel
    pass
