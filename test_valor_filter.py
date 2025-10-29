#!/usr/bin/env python3
"""Teste para verificar filtro de valor"""
from app import app, db_manager
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)

def test_valor_filter():
    print('🧪 Testando filtro de valor com slider...')
    
    with app.test_client() as client:
        # Carregar página inicial
        print('1. Carregando página inicial...')
        response = client.get('/')
        
        if response.status_code == 200:
            print('✅ Página carregada com sucesso!')
            
            # Verificar se max_valor está sendo passado
            html_content = response.data.decode()
            
            if 'max_valor' in html_content:
                print('✅ max_valor está sendo passado para o template')
            else:
                print('❌ max_valor NÃO está sendo passado')
            
            # Testar filtro com valor específico
            print('\n2. Testando filtro com valor <= 100000...')
            response = client.get('/?filter_valor=100000')
            
            if response.status_code == 200:
                print('✅ Filtro aplicado com sucesso!')
                
                # Verificar se a query está correta
                html_content = response.data.decode()
                if 'registros totais' in html_content:
                    print('✅ Página renderizada corretamente')
                else:
                    print('❌ Erro ao renderizar página')
            else:
                print(f'❌ Erro ao aplicar filtro: {response.status_code}')
        else:
            print(f'❌ Erro ao carregar página: {response.status_code}')

if __name__ == "__main__":
    test_valor_filter()

