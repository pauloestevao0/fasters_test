# Teste -> Fasters

Este projeto executa scripts de tratamento e análise de dados utilizando a biblioteca **PySpark**

## Estrutura do projeto

fasters_test/
│
├── files/                  # Arquivos CSV
│   ├── base_telefonia.csv
│   ├── base_pessoas.csv
│   └── base_avaliacoes.csv
│
├── scripts/                # Scripts .py
│   ├── analise_exploratoria.py
│   └── vendas_diarias.py
│
├── output/                 # Diretório onde serão gerados arquivos e relatórios
│   ├── analise_exploratoria_YYYYMMDD.txt
│   └── vendas_diarias_YYYYMMDD/
│
└── README.md

## Scripts
### 1. `analise_exploratoria.py`
- Calcula:
  - Top 5 vendedores por valor total de vendas
  - Ticket médio por vendedor
  - Tempo médio das ligações
  - Nota média
  - Vendedor com pior média
- Gera relatório em `output/analise_exploratoria_YYYYMMDD.txt`.

### 2. `vendas_diarias.py`
- Agrupa vendas por data e líder da equipe.
- Salva resultados em formato **Parquet**, particionado por líder da equipe.

## Autor
- Paulo Estevão Taveira

