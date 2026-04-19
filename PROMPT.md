# Pipeline de Monitoramento de Preços – Smartphones

Implemente uma pipeline de dados completa para monitoramento de preços de smartphones, seguindo os requisitos abaixo.

- Mantenha o projeto simples
- Implemente apenas o necessário
- Qualquer decisão fora do escopo deve ser validada comigo antes de prosseguir

---

## 1. Coleta de dados

- Desenvolva um script em Python para realizar scraping de dados de smartphones no site da Magazine Luiza
- Os dados devem ser enviados para um tópico no Kafka
- Estruture os dados conforme o exemplo abaixo:

```json
{
  "source": "magalu",
  "site_id": "magazineluiza",
  "category_id": "smartphones",
  "search_query": "smartphone",
  "collection_run_id": "e84821ea738e53c1fa75ac56ebf193af",
  "collected_at": "2026-04-16 20:46:14.900022+00:00",
  "item_id": "240095600",
  "title": "Smartphone Samsung Galaxy A56 128GB 5G 8GB RAM Preto",
  "permalink": "...",
  "price": "1799.00",
  "original_price": "2999.99",
  "currency_id": "BRL",
  "discount_amount": "1200.99",
  "discount_pct": "10",
  "installment_count": 10,
  "installment_value": "199.89",
  "rating": "4.9",
  "review_count": 3685,
  "condition": "new",
  "brand": "Samsung",
  "storage_capacity": "128 GB",
  "ram_memory": "8 GB",
  "payload_hash": "...",
  "collected_date": "2026-04-16"
}
```

---

## 2. Ingestão (Bronze)

- Implemente um consumidor Kafka que:
  - Leia os dados do tópico
  - Persista os dados sem transformação em uma tabela Bronze no banco de dados

---

## 3. Processamento

- Implemente um processo batch que:
  - Leia os dados da camada Bronze
  - Realize limpeza e padronização dos dados (tipos, nulos, consistência)

- Salve os dados em:
  - Tabela Staging (dados limpos)
  - Camada Marts (dados modelados)

---

## 4. Modelagem (Marts)

- Modele os dados utilizando modelagem dimensional:
  - Tabelas fato
  - Tabelas dimensão

- As transformações devem incluir:
  - Pelo menos 3 testes de qualidade de dados (ex: nulos, duplicados, ranges)
  - Documentação básica dos modelos

---

## 5. Orquestração

- Utilize o Apache Airflow para orquestrar:
  - Coleta de dados
  - Consumo do Kafka
  - Carga na Bronze
  - Processamento para Staging
  - Processamento para Marts

- O pipeline deve:
  - Orquestrar todas as etapas
  - Rodar automaticamente a cada 1 hora
  - Garantir idempotência

---

## 6. Infraestrutura

- Toda a solução deve rodar em Docker, incluindo:
  - Kafka
  - Banco de dados (DuckDB)
  - Airflow
  - Scripts de coleta e processamento

- A execução deve ocorrer com um único comando:

```bash
docker compose up
```

---

## 7. Perguntas de negócio

A camada final deve permitir responder às seguintes perguntas:

1. Preço médio, mínimo e máximo dos smartphones  
2. Proporção de produtos com frete grátis (e variação por condição)  
3. Top 10 vendedores por volume de vendas  
4. Correlação entre desconto e quantidade vendida  
5. Evolução do preço médio ao longo do tempo  
6. Distribuição por condição (novo vs usado) e ticket médio  
7. Produtos com maior variação de preço  
8. Comparação de preço médio com/sem frete grátis  
9. Distribuição de preços (histograma em faixas de R$ 500)  
10. Vendedores com melhor equilíbrio entre preço e volume  

---

## 8. Configuração

- Utilize um arquivo `.env` na raiz do projeto para variáveis de ambiente

