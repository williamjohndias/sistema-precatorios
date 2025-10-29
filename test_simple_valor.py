from app import app, db_manager

with app.test_client() as client:
    response = client.get('/')
    print(f'Status: {response.status_code}')
    
    if 'Valor (máx:' in response.data.decode():
        print('✅ Campo de valor está no HTML')
    else:
        print('❌ Campo de valor NÃO está no HTML')
