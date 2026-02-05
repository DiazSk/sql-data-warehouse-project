# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "marimo",
#     "duckdb",
#     "pandas",
#     "plotly",
#     "numpy",
# ]
# ///
"""
Olist E-Commerce Analytics Dashboard (Demo Version)
====================================================
Self-contained demo with embedded sample data.
Run: uvx marimo edit olist_demo_dashboard.py
"""

import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # 🛒 Olist E-Commerce Data Warehouse
    ## Interactive Analytics Dashboard

    **Architecture:** Medallion (Bronze → Silver → Gold) | **Schema:** Star Schema

    **Dataset:** 99K orders | 112K line items | Brazilian E-Commerce | 2016-2018

    ---
    """)
    return


@app.cell
def _():
    import pandas as pd
    import numpy as np
    import duckdb
    import plotly.express as px
    import plotly.graph_objects as go
    from datetime import datetime, timedelta
    return datetime, duckdb, go, np, pd, px, timedelta


@app.cell
def _(datetime, np, pd, timedelta):
    # ============================================================
    # GOLD LAYER: Star Schema (Simulated from real Olist structure)
    # ============================================================
    np.random.seed(42)

    # Brazilian regions mapping (exact match to production schema)
    REGIONS = {
        'SP': 'Southeast', 'RJ': 'Southeast', 'MG': 'Southeast', 'ES': 'Southeast',
        'RS': 'South', 'PR': 'South', 'SC': 'South',
        'BA': 'Northeast', 'PE': 'Northeast', 'CE': 'Northeast', 'MA': 'Northeast',
        'GO': 'Central-West', 'DF': 'Central-West', 'MT': 'Central-West',
        'PA': 'North', 'AM': 'North'
    }

    # Product categories (English translations from production)
    CATEGORIES = [
        'bed_bath_table', 'health_beauty', 'sports_leisure', 'furniture_decor',
        'computers_accessories', 'housewares', 'watches_gifts', 'telephony',
        'garden_tools', 'auto', 'cool_stuff', 'perfumery', 'toys',
        'electronics', 'fashion_bags_accessories', 'stationery', 'construction_tools_safety'
    ]

    PAYMENT_TYPES = ['credit_card', 'boleto', 'debit_card', 'voucher']
    STATES = list(REGIONS.keys())

    # Generate realistic order dates (2017-2018, matching Olist timeframe)
    n_orders = 25000
    start_date = datetime(2017, 1, 1)
    end_date = datetime(2018, 8, 31)
    date_range = (end_date - start_date).days

    order_dates = [start_date + timedelta(days=np.random.randint(0, date_range)) for _ in range(n_orders)]

    # State distribution (SP dominates in real data)
    state_weights = [0.415, 0.13, 0.12, 0.03, 0.06, 0.05, 0.04, 0.05, 0.03, 0.02, 0.02, 0.01, 0.01, 0.005, 0.005, 0.005]

    # fact_orders (Order-level fact table)
    fact_orders = pd.DataFrame({
        'order_id': [f'ord_{i:06d}' for i in range(n_orders)],
        'order_purchase_timestamp': order_dates,
        'order_status': np.random.choice(
            ['delivered', 'shipped', 'canceled', 'invoiced', 'processing'],
            n_orders, p=[0.92, 0.03, 0.02, 0.02, 0.01]
        ),
        'customer_state': np.random.choice(STATES, n_orders, p=state_weights),
        'total_items': np.random.choice([1, 2, 3, 4, 5], n_orders, p=[0.6, 0.25, 0.1, 0.03, 0.02]),
        'total_product_value': np.random.lognormal(4.5, 0.8, n_orders).round(2),
        'total_freight_value': np.random.uniform(15, 60, n_orders).round(2),
        'payment_type': np.random.choice(PAYMENT_TYPES, n_orders, p=[0.74, 0.18, 0.05, 0.03]),
        'payment_installments': np.random.choice([1, 2, 3, 4, 5, 6, 10, 12], n_orders, p=[0.5, 0.15, 0.1, 0.08, 0.07, 0.05, 0.03, 0.02]),
        'review_score': np.random.choice([1, 2, 3, 4, 5], n_orders, p=[0.08, 0.03, 0.08, 0.24, 0.57]),
        'delivery_days': np.maximum(1, np.random.poisson(12, n_orders)),
    })

    # Derived columns
    fact_orders['total_order_value'] = fact_orders['total_product_value'] + fact_orders['total_freight_value']
    fact_orders['customer_region'] = fact_orders['customer_state'].map(REGIONS)
    fact_orders['order_month'] = pd.to_datetime(fact_orders['order_purchase_timestamp']).dt.to_period('M').astype(str)
    fact_orders['order_year'] = pd.to_datetime(fact_orders['order_purchase_timestamp']).dt.year
    fact_orders['order_date_key'] = pd.to_datetime(fact_orders['order_purchase_timestamp']).dt.strftime('%Y%m%d').astype(int)

    estimated_days = np.random.uniform(10, 25, n_orders)
    fact_orders['is_late'] = fact_orders['delivery_days'] > estimated_days

    # fact_order_items (Line item fact table)
    n_items = int(n_orders * 1.4)

    fact_order_items = pd.DataFrame({
        'order_id': np.random.choice(fact_orders['order_id'], n_items),
        'order_item_id': np.tile(np.arange(1, 6), n_items // 5 + 1)[:n_items],
        'product_category_name_english': np.random.choice(CATEGORIES, n_items),
        'seller_state': np.random.choice(STATES, n_items, p=state_weights),
        'price': np.random.lognormal(4.2, 0.9, n_items).round(2),
        'freight_value': np.random.uniform(10, 45, n_items).round(2),
    })

    fact_order_items['item_total'] = fact_order_items['price'] + fact_order_items['freight_value']
    fact_order_items['seller_region'] = fact_order_items['seller_state'].map(REGIONS)

    order_lookup = fact_orders.set_index('order_id')[['order_month', 'order_date_key', 'customer_region']].to_dict()
    fact_order_items['order_month'] = fact_order_items['order_id'].map(order_lookup['order_month'])

    print(f"✅ Generated {len(fact_orders):,} orders and {len(fact_order_items):,} line items")
    return CATEGORIES, fact_orders


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🎛️ Interactive Filters
    """)
    return


@app.cell
def _(CATEGORIES, fact_orders, mo):
    years = sorted(fact_orders['order_year'].unique().tolist())
    regions = ['All'] + sorted(fact_orders['customer_region'].dropna().unique().tolist())
    categories = ['All'] + sorted(CATEGORIES)
    statuses = ['All'] + sorted(fact_orders['order_status'].unique().tolist())

    year_selector = mo.ui.dropdown(
        options={str(y): y for y in years},
        value='2018',
        label='📅 Year'
    )

    region_filter = mo.ui.dropdown(
        options=regions,
        value='All',
        label='🗺️ Region'
    )

    category_filter = mo.ui.dropdown(
        options=categories,
        value='All',
        label='📦 Category'
    )

    status_filter = mo.ui.dropdown(
        options=statuses,
        value='All',
        label='📋 Status'
    )

    mo.hstack([year_selector, region_filter, category_filter, status_filter], justify='start', gap=2)
    return category_filter, region_filter, status_filter, year_selector


@app.cell
def _(category_filter, duckdb, region_filter, status_filter, year_selector):
    selected_year = int(year_selector.value)
    selected_region = region_filter.value
    selected_category = category_filter.value
    selected_status = status_filter.value

    orders_query = f"""
    SELECT * FROM fact_orders
    WHERE order_year = {selected_year}
    """
    if selected_region != 'All':
        orders_query += f" AND customer_region = '{selected_region}'"
    if selected_status != 'All':
        orders_query += f" AND order_status = '{selected_status}'"

    filtered_orders = duckdb.sql(orders_query).df()

    items_query = f"""
    SELECT * FROM fact_order_items
    WHERE order_month LIKE '{selected_year}%'
    """
    if selected_category != 'All':
        items_query += f" AND product_category_name_english = '{selected_category}'"
    if selected_region != 'All':
        items_query += f" AND seller_region = '{selected_region}'"

    filtered_items = duckdb.sql(items_query).df()
    return (filtered_orders,)


@app.cell
def _(duckdb, mo):
    kpis = duckdb.sql("""
        SELECT 
            COUNT(*) as total_orders,
            ROUND(SUM(total_order_value), 2) as total_revenue,
            ROUND(AVG(total_order_value), 2) as avg_order_value,
            ROUND(AVG(review_score), 2) as avg_review,
            ROUND(AVG(delivery_days), 1) as avg_delivery,
            ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 1) as late_pct
        FROM filtered_orders
        WHERE order_status = 'delivered'
    """).df()

    mo.md(f"""
    ## 📊 Key Performance Indicators

    | Orders | Revenue (BRL) | Avg Order | Avg Review | Delivery Days | Late % |
    |:------:|:-------------:|:---------:|:----------:|:-------------:|:------:|
    | **{kpis['total_orders'].iloc[0]:,}** | **R$ {kpis['total_revenue'].iloc[0]:,.0f}** | **R$ {kpis['avg_order_value'].iloc[0]:.0f}** | **{kpis['avg_review'].iloc[0]:.1f} ⭐** | **{kpis['avg_delivery'].iloc[0]:.0f}** | **{kpis['late_pct'].iloc[0]:.1f}%** |
    """)
    return


@app.cell
def _(duckdb, mo, px):
    monthly = duckdb.sql("""
        SELECT 
            order_month,
            COUNT(*) as orders,
            ROUND(SUM(total_order_value), 0) as revenue
        FROM filtered_orders
        WHERE order_status = 'delivered'
        GROUP BY order_month
        ORDER BY order_month
    """).df()

    fig_trend = px.area(
        monthly, x='order_month', y='revenue',
        title='📈 Monthly Revenue Trend',
        labels={'order_month': 'Month', 'revenue': 'Revenue (BRL)'},
        color_discrete_sequence=['#2E86AB']
    )
    fig_trend.update_layout(hovermode='x unified')

    return mo.ui.plotly(fig_trend)


@app.cell
def _(duckdb, mo, px):
    by_region = duckdb.sql("""
        SELECT 
            customer_region,
            COUNT(*) as orders,
            ROUND(SUM(total_order_value), 0) as revenue,
            ROUND(AVG(review_score), 2) as avg_review
        FROM filtered_orders
        WHERE order_status = 'delivered'
        GROUP BY customer_region
        ORDER BY revenue DESC
    """).df()

    fig_region = px.bar(
        by_region, x='customer_region', y='revenue',
        color='avg_review', color_continuous_scale='RdYlGn',
        title='🗺️ Revenue by Brazilian Region',
        labels={'customer_region': 'Region', 'revenue': 'Revenue (BRL)'},
        text='orders'
    )
    fig_region.update_traces(texttemplate='%{text:,}', textposition='outside')

    return mo.ui.plotly(fig_region)


@app.cell
def _(duckdb, mo, px):
    by_category = duckdb.sql("""
        SELECT 
            product_category_name_english as category,
            COUNT(*) as items,
            ROUND(SUM(item_total), 0) as revenue
        FROM filtered_items
        GROUP BY product_category_name_english
        ORDER BY revenue DESC
        LIMIT 10
    """).df()

    fig_cat = px.bar(
        by_category, y='category', x='revenue', orientation='h',
        color='items', color_continuous_scale='Blues',
        title='📦 Top 10 Product Categories',
        labels={'category': 'Category', 'revenue': 'Revenue (BRL)'}
    )
    fig_cat.update_layout(yaxis={'categoryorder': 'total ascending'})

    return mo.ui.plotly(fig_cat)


@app.cell
def _(duckdb, go, mo):
    by_payment = duckdb.sql("""
        SELECT 
            payment_type,
            COUNT(*) as orders,
            ROUND(SUM(total_order_value), 0) as revenue
        FROM filtered_orders
        GROUP BY payment_type
        ORDER BY orders DESC
    """).df()

    fig_pay = go.Figure(data=[go.Pie(
        labels=by_payment['payment_type'],
        values=by_payment['orders'],
        hole=0.45,
        textinfo='label+percent',
        marker_colors=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
    )])
    fig_pay.update_layout(title='💳 Payment Type Distribution')

    return mo.ui.plotly(fig_pay)


@app.cell
def _(duckdb, mo, px):
    by_review = duckdb.sql("""
        SELECT 
            review_score,
            COUNT(*) as orders
        FROM filtered_orders
        WHERE review_score IS NOT NULL
        GROUP BY review_score
        ORDER BY review_score
    """).df()

    fig_review = px.bar(
        by_review, x='review_score', y='orders',
        color='review_score', color_continuous_scale='RdYlGn',
        title='⭐ Review Score Distribution',
        text='orders'
    )
    fig_review.update_traces(texttemplate='%{text:,}', textposition='outside')
    fig_review.update_layout(showlegend=False)

    return mo.ui.plotly(fig_review)


@app.cell
def _(duckdb, mo, px):
    delivery = duckdb.sql("""
        SELECT 
            customer_region,
            ROUND(AVG(delivery_days), 1) as avg_days,
            ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 1) as late_pct,
            COUNT(*) as orders
        FROM filtered_orders
        WHERE order_status = 'delivered'
        GROUP BY customer_region
    """).df()

    fig_del = px.scatter(
        delivery, x='avg_days', y='late_pct', size='orders',
        color='customer_region',
        title='🚚 Delivery Performance by Region',
        labels={'avg_days': 'Avg Delivery Days', 'late_pct': 'Late Delivery %'}
    )

    return mo.ui.plotly(fig_del)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🔍 Data Explorer
    """)
    return


@app.cell
def _(filtered_orders, mo):
    display_cols = ['order_id', 'order_status', 'customer_region', 'total_order_value', 
                    'payment_type', 'review_score', 'delivery_days', 'is_late', 'order_month']
    return mo.ui.dataframe(filtered_orders[display_cols].head(100))


@app.cell
def _(mo):
    mo.md("---\n## 🏗️ Data Warehouse Architecture")
    return


@app.cell
def _(mo):
    mo.image(src="../docs/data_warehouse_architecture.png", alt="Olist Data Warehouse Architecture", rounded=True)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Star Schema Tables

    | Dimensions | Facts |
    |------------|-------|
    | `dim_date` | `fact_orders` |
    | `dim_customer` | `fact_order_items` |
    | `dim_seller` | |
    | `dim_product` | |
    | `dim_geography` | |

    ---

    *Built with marimo 🍃 | Data: Brazilian E-Commerce (Olist)*
    """)
    return


if __name__ == "__main__":
    app.run()
