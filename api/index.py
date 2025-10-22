# Arquivo de configuração para Vercel
# Este arquivo permite que o Vercel execute nossa aplicação Flask

from app import app

# Exportar a aplicação para o Vercel
application = app

if __name__ == "__main__":
    app.run(debug=True)
