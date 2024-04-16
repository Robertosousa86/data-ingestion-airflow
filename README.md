# Data Ingestion Airflow

## Objetivo

Desenvolver um sistema eficiente e preciso usando Apache Airflow e Python para coletar, processar e analisar dados do mercado de ações utilizando a API Polygon.io.

Funcionalidades Principais
Automatização de Coleta de Dados: Utilização do Apache Airflow para automatizar a coleta de dados do mercado de ações de fontes diversas.

Processamento Eficiente: Implementação de pipelines eficientes para processar grandes volumes de dados, reduzindo atrasos e erros humanos.

Análise em Tempo Real: Capacidade de fornecer análises atualizadas em tempo real para permitir decisões ágeis.

Interface Intuitiva: Desenvolvimento de uma interface intuitiva para acessar insights e recomendações de forma fácil e rápida.

# DAG: stocks_dag

Esta DAG é responsável por agendar, requisitar, limpar e carregar dados da API Polygon relacionados a ações do mercado financeiro. Ela realiza as seguintes tarefas:

1. **Esperar pelo serviço**: Utiliza um sensor HTTP para aguardar que o serviço da API Polygon esteja disponível.
2. **Esperar pelo banco de dados**: Utiliza um sensor SQL para esperar que o banco de dados PostgreSQL esteja pronto para receber conexões.
3. **Inserir dados**: Realiza a requisição à API Polygon para obter dados de ações, limpa e formata esses dados e, em seguida, os insere em uma tabela no banco de dados.
4. **Inserir indicadores**: Calcula indicadores técnicos como Média Móvel Simples (SMA), Índice de Força Relativa (RSI) e Bandas de Bollinger com base nos dados inseridos anteriormente e os insere em outra tabela no banco de dados.

## Pré-requisitos

- Airflow 2.x
- Banco de dados PostgreSQL
- Chave da API Polygon configurada como variável Airflow (nome: `polygon_key`)
- Conexão Airflow para o banco de dados PostgreSQL

## Instalação

1. Clone este repositório para o seu ambiente Airflow.
2. Certifique-se de que as dependências listadas no arquivo `requirements.txt` estejam instaladas.
3. Configure as variáveis Airflow necessárias, incluindo a chave da API Polygon (`polygon_key`).
4. Configure uma conexão Airflow para o banco de dados PostgreSQL (`my_postgres_conn`).

## Utilização

- A DAG está programada para ser executada diariamente (`@daily`).
- Certifique-se de que o serviço da API Polygon esteja disponível e o banco de dados PostgreSQL esteja pronto antes de executar a DAG.
