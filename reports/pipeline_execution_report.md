# Relatório: Pipeline de Preços Magalu

## Resumo Executivo
- Fonte final: Magazine Luiza
- Arquivo bronze: `data/bronze/magalu_smartphones.jsonl`
- Total de eventos no bronze: 2733
- Produtos únicos: 737
- Marcas identificadas: 10
- Vendedores identificados: 108
- Status da validação: aprovado

## Preços
- Menor preço: R$ 11.70
- Preço médio: R$ 2962.91
- Preço mediano: R$ 1979.10
- Maior preço: R$ 20718.76
- Produtos com desconto identificado: 2664
- Desconto médio identificado: 10.81%

## Cobertura Dos Campos
| Campo | Registros preenchidos | Cobertura |
|---|---:|---:|
| `item_id` | 2733 | 100.0% |
| `title` | 2733 | 100.0% |
| `price` | 2733 | 100.0% |
| `original_price` | 2367 | 86.6% |
| `pix_price` | 2733 | 100.0% |
| `discount_pct` | 2664 | 97.5% |
| `seller_nickname` | 2733 | 100.0% |
| `free_shipping` | 0 | 0.0% |
| `condition` | 2733 | 100.0% |
| `sold_quantity` | 0 | 0.0% |
| `review_count` | 2193 | 80.2% |
| `collected_at` | 2733 | 100.0% |
| `brand` | 2645 | 96.8% |
| `storage_capacity` | 2566 | 93.9% |
| `ram_memory` | 1491 | 54.6% |
| `payload_hash` | 2733 | 100.0% |

## Checks De Qualidade
| Check | Resultado |
|---|---|
| `has_minimum_rows` | OK |
| `all_rows_from_magalu` | OK |
| `all_have_item_id` | OK |
| `all_have_title` | OK |
| `all_have_price` | OK |
| `all_have_currency` | OK |
| `all_have_collected_at` | OK |
| `all_have_condition` | OK |
| `all_have_payload_hash` | OK |
| `no_duplicate_payload_hash` | OK |
| `has_brand_coverage` | OK |
| `has_review_count_proxy` | OK |

## Top Marcas
| Marca | Eventos |
|---|---:|
| Samsung | 1000 |
| Xiaomi | 592 |
| Apple | 483 |
| Motorola | 477 |
| OPPO | 77 |
| Infinix | 8 |
| TCL | 3 |
| LG | 3 |
| Realme | 1 |
| Nokia | 1 |

## Top Vendedores
| Vendedor | Eventos |
|---|---:|
| magazineluiza | 1392 |
| lojamotorolaoficial | 105 |
| samsung | 102 |
| trocafy | 102 |
| xiaomioficial | 101 |
| lojaiplace | 93 |
| atacadoimport | 93 |
| cacvariedades | 73 |
| seloprofissional | 39 |
| kabum | 33 |

## Queries Coletadas
| Query | Eventos |
|---|---:|
| smartphone | 378 |
| celular 5g | 371 |
| samsung galaxy | 352 |
| galaxy | 346 |
| iphone | 341 |
| redmi | 341 |
| xiaomi | 318 |
| motorola | 286 |

## Tabelas Do Pipeline
| Tabela | Linhas |
|---|---:|
| `bronze_product_listing_events` | 2733 |
| `raw_product_listing_events` | 1143 |
| `staging_product_listings` | 2545 |
| `staging_product_current` | 661 |
| `dim_products` | 661 |
| `dim_brands` | 11 |
| `dim_sellers` | 89 |
| `dim_conditions` | 1 |
| `fact_product_prices` | 2545 |
| `mart_price_summary` | 3 |
| `mart_free_shipping_by_condition` | 1 |
| `mart_top_sellers_by_volume` | 10 |
| `mart_discount_volume_correlation` | 1 |
| `mart_avg_price_evolution` | 3 |
| `mart_condition_distribution` | 1 |
| `mart_price_variation_products` | 20 |
| `mart_free_shipping_price_comparison` | 1 |
| `mart_price_bands` | 31 |
| `mart_seller_price_volume_balance` | 10 |
| `mart_top_reviewed_products` | 20 |

## Última Execução Auditada
- `collection_run_id`: `bf7eae9946e5e998967e16257f9aade9`
- Status: `success`
- Início: `2026-04-18T22:00:05.235804+00:00`
- Fim: `2026-04-18T22:01:21.043392+00:00`
- Métricas: `{"total_pages": 16, "failed_pages": 0, "total_cards": 635, "total_invalid_events": 0, "total_valid_events_written": 95, "total_skipped_existing_payload_hash": 540, "total_events_published": 635}`

## Limitações Conhecidas
- O Magalu não expõe `sold_quantity` de forma consistente na busca pública; `review_count` foi mantido como proxy de tração.
- Os marts de volume usam `sales_volume_proxy = coalesce(sold_quantity, review_count)`.
- `free_shipping` depende do texto exibido no card; quando ausente, o campo fica nulo e é tratado nas análises.
- A extração usa páginas públicas, por isso mudanças no HTML da fonte podem exigir ajustes no parser.
