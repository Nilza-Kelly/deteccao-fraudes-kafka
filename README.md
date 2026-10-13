
# Sistema de Detecção de Fraudes com Apache Kafka

Projeto acadêmico de detecção de transações financeiras suspeitas
utilizando Apache Kafka, Python e PostgreSQL.

## Objetivo

Simular transações financeiras, processar mensagens em tempo real,
identificar comportamentos suspeitos e armazenar todas as transações
em um banco de dados.

## Regras de detecção

- ALTO_VALOR: transação com valor maior ou igual a R$ 10.000.
- TEMPO_60s: quatro ou mais transações do mesmo cliente em uma
  janela inferior a 60 segundos.
- GEO_10m: transações do mesmo cliente em cidades diferentes
  dentro de uma janela de 10 minutos.

## Tecnologias

- Python
- Apache Kafka
- PostgreSQL
- Docker Compose
- Git e GitHub

## Infraestrutura

O Docker Compose será utilizado para executar o Kafka e o PostgreSQL
localmente.

## Execução

A configuração inicial da infraestrutura está no arquivo
docker-compose.yml.

As instruções completas de instalação e execução serão adicionadas
durante o desenvolvimento.
