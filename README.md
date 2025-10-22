# 🚀 Sistema de Precatórios - Deploy Vercel

Esta pasta contém **APENAS** os arquivos necessários para deploy no Vercel.

## 📁 Estrutura de Arquivos

```
vercelDeploy/
├── app.py                 # Aplicação Flask otimizada para Vercel
├── requirements.txt       # Dependências Python
├── vercel.json           # Configuração específica do Vercel
├── .gitignore            # Arquivos ignorados pelo Git
├── templates/            # Templates HTML
│   ├── index.html
│   ├── logs.html
│   └── error.html
├── static/               # Arquivos estáticos
│   ├── script.js
│   └── style.css
└── api/                  # Entry point para Vercel
    └── index.py
```

## 🚀 Deploy no Vercel

### 1. Subir para GitHub
```bash
cd vercelDeploy
git init
git add .
git commit -m "Sistema de Precatórios - Vercel"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git
git push -u origin main
```

### 2. Deploy no Vercel
1. Acesse [vercel.com](https://vercel.com)
2. Login com GitHub
3. "New Project"
4. Selecione seu repositório
5. Configure as variáveis de ambiente:

```
DB_HOST=bdunicoprecs.c50cwuocuwro.sa-east-1.rds.amazonaws.com
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=$P^iFe27^YP5cpBU3J&tqa
DB_NAME=OCSC
SECRET_KEY=sua_chave_secreta_forte_aqui_123456
```

6. Deploy automático!

## ⚙️ Configurações Importantes

- **Framework Preset:** Other
- **Build Command:** (deixar vazio)
- **Output Directory:** (deixar vazio)
- **Install Command:** pip install -r requirements.txt
- **Max Duration:** Padrão do Vercel (sem configuração específica)

## ✅ Verificação

Após o deploy, teste:
- ✅ Página principal carrega
- ✅ Dados aparecem na tabela
- ✅ Filtros funcionam
- ✅ Edição individual funciona
- ✅ Edição em massa funciona

## 🚨 Solução de Problemas

### Erro: "FUNCTION_INVOCATION_FAILED"
- Verifique logs no Vercel
- Confirme variáveis de ambiente
- Teste conexão com banco

### Erro: "Build Failed"
- Verifique requirements.txt
- Confirme dependências

### Erro: "Database Connection"
- Verifique variáveis de ambiente
- Confirme se banco aceita conexões externas

---

**Nota:** Esta pasta contém apenas os arquivos essenciais para deploy no Vercel. O projeto original permanece na pasta pai.
