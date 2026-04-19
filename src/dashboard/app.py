from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.queries import DashboardDataError, load_dashboard_data


DEFAULT_DB_PATH = "data/warehouse/smartphone_price_intelligence.duckdb"
LOCAL_TIMEZONE = ZoneInfo(os.environ.get("TZ", "America/Fortaleza"))
COLUMN_LABELS = {
    "item_id": "ID do produto",
    "title": "Produto",
    "brand": "Marca",
    "condition": "Condição",
    "seller_nickname": "Vendedor",
    "product_count": "Quantidade de produtos",
    "observations": "Observações",
    "free_shipping_count": "Frete grátis",
    "paid_shipping_count": "Frete pago",
    "unknown_shipping_count": "Sem informação de frete",
    "free_shipping_pct_known": "% frete grátis entre conhecidos",
    "total_sales_volume_proxy": "Volume total proxy",
    "avg_price": "Preço médio",
    "min_price": "Preço mínimo",
    "max_price": "Preço máximo",
    "avg_discount_pct": "Desconto médio",
    "discount_pct": "Desconto",
    "sales_volume_proxy": "Volume proxy",
    "discount_sales_volume_proxy_correlation": "Correlação desconto-volume",
    "avg_sales_volume_proxy": "Volume proxy médio",
    "collected_date": "Data da coleta",
    "avg_ticket": "Ticket médio",
    "first_collected_at": "Primeira coleta",
    "last_collected_at": "Última coleta",
    "first_price": "Preço inicial",
    "last_price": "Preço final",
    "price_variation": "Variação de preço",
    "absolute_price_variation": "Variação absoluta",
    "free_shipping_status": "Status do frete",
    "price_band_start": "Início da faixa",
    "price_band_end": "Fim da faixa",
    "price_band_label": "Faixa de preço",
    "price_volume_balance_score": "Pontuação preço-volume",
}
DATE_COLUMNS = {"collected_date", "first_collected_at", "last_collected_at"}
INTEGER_COLUMNS = {
    "observations",
    "product_count",
    "free_shipping_count",
    "paid_shipping_count",
    "unknown_shipping_count",
    "total_sales_volume_proxy",
    "sales_volume_proxy",
    "total_events",
    "total_bronze_items",
    "total_collection_runs",
}
MONEY_COLUMNS = {
    "avg_price",
    "min_price",
    "max_price",
    "price",
    "avg_ticket",
    "first_price",
    "last_price",
    "price_variation",
    "absolute_price_variation",
    "price_band_start",
    "price_band_end",
}
PERCENT_COLUMNS = {"discount_pct", "avg_discount_pct", "free_shipping_pct_known"}
DECIMAL_COLUMNS = {
    "discount_sales_volume_proxy_correlation",
    "avg_sales_volume_proxy",
    "price_volume_balance_score",
}


def money(value: Any) -> str:
    if value is None:
        return "N/D"
    try:
        formatted = f"R$ {float(value):,.2f}"
    except (TypeError, ValueError):
        return "N/D"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")


def number(value: Any) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{float(value):,.0f}".replace(",", ".")
    except (TypeError, ValueError):
        return "N/D"


def money_without_cents(value: Any) -> str:
    if value is None:
        return "N/D"
    try:
        formatted = f"R$ {float(value):,.0f}"
    except (TypeError, ValueError):
        return "N/D"
    return formatted.replace(",", ".")


def percent(value: Any) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{float(value) * 100:.1f}%".replace(".", ",")
    except (TypeError, ValueError):
        return "N/D"


def decimal(value: Any, *, digits: int = 3) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{float(value):,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return "N/D"


def date_time(value: Any) -> str:
    if value is None:
        return "N/D"
    raw = str(value).strip()
    if not raw:
        return "N/D"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(LOCAL_TIMEZONE)
        return parsed.strftime("%d/%m/%Y %H:%M:%S")
    except ValueError:
        return raw


def frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def format_cell(column: str, value: Any) -> str:
    if pd.isna(value):
        return "N/D"
    if column in MONEY_COLUMNS:
        return money(value)
    if column in PERCENT_COLUMNS:
        return percent(float(value) / 100 if column != "free_shipping_pct_known" else value)
    if column in INTEGER_COLUMNS:
        return number(value)
    if column in DECIMAL_COLUMNS:
        return decimal(value)
    if column in DATE_COLUMNS:
        return date_time(value)
    return str(value)


def display_frame(df: pd.DataFrame) -> pd.DataFrame:
    formatted = df.copy()
    for column in formatted.columns:
        formatted[column] = formatted[column].map(lambda value, col=column: format_cell(col, value))
    formatted = formatted.rename(columns=COLUMN_LABELS)
    return formatted


def plot_chart(fig: Any) -> None:
    fig.update_layout(separators=",.", margin={"l": 10, "r": 10, "t": 30, "b": 10})
    st.plotly_chart(fig, width="stretch")


def is_lock_error(error: Exception) -> bool:
    message = str(error).lower()
    return "conflicting lock" in message or "could not set lock" in message or "lock" in message


@st.cache_data(ttl=60, show_spinner=False)
def cached_dashboard_data(db_path: str) -> dict[str, Any]:
    return load_dashboard_data(db_path)


def show_empty(message: str = "Ainda não há dados suficientes para esta resposta.") -> None:
    st.info(message)


def methodology(text: str) -> None:
    st.caption(f"Metodologia de cálculo: {text}")


def horizontal_bar(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    x_label: str,
    y_label: str,
    title: str | None = None,
    color: str | None = None,
    color_label: str | None = None,
) -> None:
    if df.empty:
        show_empty()
        return
    labels = {x: x_label, y: y_label}
    if color and color_label:
        labels[color] = color_label
    fig = px.bar(df, x=x, y=y, orientation="h", color=color, title=title, labels=labels)
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=max(360, 34 * len(df)))
    plot_chart(fig)


def main() -> None:
    st.set_page_config(page_title="Preços Magalu", page_icon=":bar_chart:", layout="wide")

    db_path = Path(os.environ.get("DB_PATH", DEFAULT_DB_PATH))

    st.title("Monitoramento de preços Magalu")
    st.caption("Dados mais recentes persistidos pela pipeline Airflow/Kafka/DuckDB.")

    with st.sidebar:
        st.header("Dados")
        st.write("Banco DuckDB")
        st.code(str(db_path))
        if st.button("Atualizar dados", width="stretch"):
            st.cache_data.clear()
            st.rerun()

    if not db_path.exists():
        st.warning(
            "O arquivo DuckDB ainda não existe. Execute a DAG `price_monitoring_smartphones` "
            "no Airflow ou rode `python -m src.orchestration.cli` para materializar os dados."
        )
        return

    try:
        data = cached_dashboard_data(str(db_path))
    except DashboardDataError as exc:
        if is_lock_error(exc):
            st.warning(
                "Não consegui abrir o DuckDB agora porque ele parece estar bloqueado. "
                "Feche a conexão no DBeaver ou aguarde o Airflow terminar a escrita e clique em \"Atualizar dados\"."
            )
        else:
            st.error(f"Não foi possível carregar os dados do dashboard: {exc}")
        return

    last_collection = data["last_collection"]
    with st.sidebar:
        st.write("Última coleta")
        st.caption("Horário local")
        st.code(date_time(last_collection.get("last_collected_at")))
        st.metric("Eventos Bronze", number(last_collection.get("total_events")))
        st.metric("Itens Bronze", number(last_collection.get("total_bronze_items")))

    price_kpis = data["price_kpis"]
    st.header("1. Qual é o preço médio, mínimo e máximo de smartphones na plataforma?")
    col_avg, col_min, col_max, col_count = st.columns(4)
    col_avg.metric("Preço médio", money(price_kpis.get("avg_price")))
    col_min.metric("Preço mínimo", money(price_kpis.get("min_price")))
    col_max.metric("Preço máximo", money(price_kpis.get("max_price")))
    col_count.metric("Produtos atuais", number(price_kpis.get("product_count")))

    st.header("2. Qual proporção dos produtos oferece frete grátis? Isso varia entre produtos novos e usados?")
    methodology("contagem dos produtos atuais por condição e status de frete. Quando a busca pública não expõe frete, o status fica como sem informação.")
    shipping = frame(data["free_shipping_by_condition"])
    if shipping.empty:
        show_empty()
    else:
        melted = shipping.melt(
            id_vars=["condition"],
            value_vars=["free_shipping_count", "paid_shipping_count", "unknown_shipping_count"],
            var_name="status",
            value_name="shipping_count",
        )
        labels = {
            "free_shipping_count": "Frete grátis",
            "paid_shipping_count": "Frete pago",
            "unknown_shipping_count": "Sem informação",
        }
        melted["status"] = melted["status"].map(labels)
        fig = px.bar(
            melted,
            x="condition",
            y="shipping_count",
            color="status",
            barmode="group",
            labels={
                "condition": "Condição do produto",
                "shipping_count": "Quantidade de produtos",
                "status": "Status do frete",
            },
        )
        plot_chart(fig)
        st.dataframe(display_frame(shipping), width="stretch", hide_index=True)

    st.header("3. Quais são os 10 vendedores com maior volume total de produtos vendidos?")
    methodology("ranking por soma de `sales_volume_proxy`, calculado como `coalesce(sold_quantity, review_count)`. Quando vendas reais não vêm da busca pública, avaliações entram como proxy de volume.")
    sellers = frame(data["top_sellers_by_volume"])
    horizontal_bar(
        sellers,
        x="total_sales_volume_proxy",
        y="seller_nickname",
        x_label="Volume total proxy",
        y_label="Vendedor",
    )
    if not sellers.empty:
        st.dataframe(display_frame(sellers), width="stretch", hide_index=True)

    st.header("4. Existe correlação entre o percentual de desconto oferecido e a quantidade vendida?")
    correlation = data["discount_volume_correlation"]
    corr_value = correlation.get("discount_sales_volume_proxy_correlation")
    st.metric("Correlação desconto vs volume", "N/D" if corr_value is None else decimal(corr_value))
    methodology("correlação de Pearson entre `discount_pct` e `sales_volume_proxy`. O volume segue `coalesce(sold_quantity, review_count)`.")
    scatter = frame(data["discount_volume_scatter"])
    if scatter.empty:
        show_empty()
    else:
        fig = px.scatter(
            scatter,
            x="discount_pct",
            y="sales_volume_proxy",
            size="price",
            hover_data=["title", "seller_nickname", "price"],
            labels={
                "discount_pct": "Desconto (%)",
                "sales_volume_proxy": "Volume proxy",
                "price": "Preço atual",
                "title": "Produto",
                "seller_nickname": "Vendedor",
            },
        )
        plot_chart(fig)

    st.header("5. Como o preço médio da categoria evoluiu ao longo dos dias coletados?")
    methodology("média diária de preço em `fact_product_prices`, usando todas as observações coletadas em cada data.")
    evolution = frame(data["avg_price_evolution"])
    if evolution.empty:
        show_empty()
    else:
        fig = px.line(
            evolution,
            x="collected_date",
            y="avg_price",
            markers=True,
            labels={"collected_date": "Data da coleta", "avg_price": "Preço médio"},
        )
        fig.update_yaxes(tickprefix="R$ ")
        plot_chart(fig)
        st.dataframe(display_frame(evolution), width="stretch", hide_index=True)

    st.header("6. Qual a distribuição dos produtos por condição (novo vs. usado) e qual grupo tem ticket médio mais alto?")
    methodology("contagem de produtos atuais por condição e média de preço em cada grupo.")
    conditions = frame(data["condition_distribution"])
    if conditions.empty:
        show_empty()
    else:
        fig = px.bar(
            conditions,
            x="condition",
            y="product_count",
            color="avg_ticket",
            labels={
                "condition": "Condição do produto",
                "product_count": "Quantidade de produtos",
                "avg_ticket": "Ticket médio",
            },
        )
        plot_chart(fig)
        st.dataframe(display_frame(conditions), width="stretch", hide_index=True)

    st.header("7. Quais produtos tiveram a maior variação de preço entre o primeiro e o último registro coletado?")
    methodology("diferença entre o último preço coletado e o primeiro preço coletado por `item_id`; o ranking usa a variação absoluta.")
    variation = frame(data["price_variation_products"])
    if variation.empty:
        show_empty()
    else:
        horizontal_bar(
            variation.head(10),
            x="absolute_price_variation",
            y="title",
            x_label="Variação absoluta de preço",
            y_label="Produto",
            color="brand",
            color_label="Marca",
        )
        st.dataframe(display_frame(variation), width="stretch", hide_index=True)

    st.header("8. Os produtos com frete grátis têm preço médio maior ou menor do que os sem frete grátis?")
    methodology("preço médio agrupado pelo status de frete. Quando o card não expõe a informação, o grupo aparece como `unknown`.")
    shipping_price = frame(data["free_shipping_price_comparison"])
    if shipping_price.empty:
        show_empty()
    else:
        fig = px.bar(
            shipping_price,
            x="free_shipping_status",
            y="avg_price",
            color="free_shipping_status",
            labels={
                "free_shipping_status": "Status do frete",
                "avg_price": "Preço médio",
            },
        )
        fig.update_yaxes(tickprefix="R$ ")
        plot_chart(fig)
        st.dataframe(display_frame(shipping_price), width="stretch", hide_index=True)

    st.header("9. Qual a faixa de preço com maior concentração de produtos (histograma em faixas de R$ 500)?")
    methodology("produtos atuais agrupados em faixas de preço de R$ 500.")
    bands = frame(data["price_bands"])
    if bands.empty:
        show_empty()
    else:
        bands["price_band_label"] = bands.apply(
            lambda row: f"{money_without_cents(row['price_band_start'])} a {money_without_cents(row['price_band_end'])}",
            axis=1,
        )
        fig = px.bar(
            bands,
            x="price_band_label",
            y="product_count",
            labels={"price_band_label": "Faixa de preço", "product_count": "Quantidade de produtos"},
        )
        plot_chart(fig)
        st.dataframe(display_frame(bands), width="stretch", hide_index=True)

    st.header("10. Quais vendedores têm o melhor equilíbrio entre preço competitivo e volume de vendas?")
    methodology("ranking por `sum(sales_volume_proxy) / avg(price)`, combinando volume proxy maior com preço médio menor.")
    balance = frame(data["seller_price_volume_balance"])
    horizontal_bar(
        balance,
        x="price_volume_balance_score",
        y="seller_nickname",
        x_label="Pontuação preço-volume",
        y_label="Vendedor",
    )
    if not balance.empty:
        st.dataframe(display_frame(balance), width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
