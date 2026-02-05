# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "marimo",
#     "duckdb",
#     "pandas",
#     "plotly",
#     "numpy",
#     "psycopg2-binary",
# ]
# ///
"""
Olist E-Commerce Analytics Dashboard (Production Version)
==========================================================
Connects to PostgreSQL Gold layer tables.
Run: uvx marimo edit olist_analytics_dashboard.py
"""

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="full")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import duckdb
    import plotly.express as px
    import plotly.graph_objects as go
    import psycopg2
    import os
    return mo, pd, np, duckdb, px, go, psycopg2, os


@app.cell
def __(mo):
    mo.md(
        """
        # 🛒 Olist E-Commerce Data Warehouse
        ## Interactive Analytics Dashboard (Production)
        
        **Connected to:** PostgreSQL Gold Layer | **Schema:** Star Schema
        
        ---
        """
    )
    return


@app.cell
def __(mo, os, pd, psycopg2):
    # Database connection parameters
    DB_CONFIG = {
        'host': os.environ.get('DWH_HOST', 'localhost'),
        'port': os.environ.get('DWH_PORT', '5433'),
        'database': os.environ.get('DWH_DATABASE', 'olist_dwh'),
        'user': os.environ.get('DWH_USER', 'olist'),
        'password': os.environ.get('DWH_PASSWORD', 'olist123')
    }
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Load fact_orders from Gold layer
        fact_orders = pd.read_sql("""
            SELECT 
                fo.order_id,
                fo.order_status,
                fo.total_items,
                fo.total_product_value,
                fo.total_freight_value,
                fo.total_order_value,
                fo.payment_type,
                fo.payment_installments,
                fo.delivery_days,
                fo.is_late,
                fo.review_score,
                dc.customer_state,
                dc.customer_region,
                dd.full_date as order_date,
                dd.year as order_year,
                dd.month_name as order_month_name,
                TO_CHAR(dd.full_date, 'YYYY-MM') as order_month
            FROM gold.fact_orders fo
            LEFT JOIN gold.dim_customer dc ON fo.customer_key = dc.customer_key
            LEFT JOIN gold.dim_date dd ON fo.order_date_key = dd.date_key
            WHERE fo.order_date_key IS NOT NULL
        """, conn)
        
        # Load fact_order_items from Gold layer
        fact_order_items = pd.read_sql("""
            SELECT 
                foi.order_id,
                foi.order_item_id,
                foi.price,
                foi.freight_value,
                foi.item_total,
                dp.category_name_en as product_category_name_english,
                ds.seller_state,
                ds.seller_region,
                TO_CHAR(dd.full_date, 'YYYY-MM') as order_month
            FROM gold.fact_order_items foi
            LEFT JOIN gold.dim_product dp ON foi.product_key = dp.product_key
            LEFT JOIN gold.dim_seller ds ON foi.seller_key = ds.seller_key
            LEFT JOIN gold.dim_date dd ON foi.order_date_key = dd.date_key
        """, conn)
        
        conn.close()
        connection_status = f"✅ Connected to PostgreSQL | Loaded {len(fact_orders):,} orders"
        
    except Exception as e:
        connection_status = f"❌ Database connection failed: {e}"
        # Fall back to empty DataFrames
        fact_orders = pd.DataFrame()
        fact_order_items = pd.DataFrame()
    
    mo.md(f"**Status:** {connection_status}")
    return DB_CONFIG, conn, fact_orders, fact_order_items, connection_status


@app.cell
def __(mo):
    mo.md("---\n## 🎛️ Interactive Filters")
    return


@app.cell
def __(fact_orders, mo):
    # Handle empty DataFrame case
    if fact_orders.empty:
        years = [2017, 2018]
        regions = ['All']
        categories = ['All']
        statuses = ['All']
    else:
        years = sorted(fact_orders['order_year'].dropna().unique().tolist())
        regions = ['All'] + sorted(fact_orders['customer_region'].dropna().unique().tolist())
        statuses = ['All'] + sorted(fact_orders['order_status'].dropna().unique().tolist())
        categories = ['All']
    
    year_selector = mo.ui.dropdown(
        options={str(y): y for y in years},
        value=str(years[-1]) if years else '2018',
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
    return (
        categories, category_filter, region_filter, regions,
        status_filter, statuses, year_selector, years,
    )


@app.cell
def __(
    category_filter, duckdb, fact_order_items, fact_orders,
    region_filter, status_filter, year_selector,
):
    if fact_orders.empty:
        filtered_orders = fact_orders
        filtered_items = fact_order_items
    else:
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
    return (
        filtered_items, filtered_orders,
    )


@app.cell
def __(duckdb, filtered_orders, mo):
    _output = mo.md("""
## 📊 Key Performance Indicators

⚠️ **No data available.** Please check database connection.
""")
    if not filtered_orders.empty:
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
        
        _output = mo.md(f"""
## 📊 Key Performance Indicators

| Orders | Revenue (BRL) | Avg Order | Avg Review | Delivery Days | Late % |
|:------:|:-------------:|:---------:|:----------:|:-------------:|:------:|
| **{kpis['total_orders'].iloc[0]:,}** | **R$ {kpis['total_revenue'].iloc[0]:,.0f}** | **R$ {kpis['avg_order_value'].iloc[0]:.0f}** | **{kpis['avg_review'].iloc[0]:.1f} ⭐** | **{kpis['avg_delivery'].iloc[0]:.0f}** | **{kpis['late_pct'].iloc[0]:.1f}%** |
""")
    _output
    return


@app.cell
def __(duckdb, filtered_orders, mo, px):
    _chart = None
    if not filtered_orders.empty:
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
        _chart = mo.ui.plotly(fig_trend)
    _chart
    return


@app.cell
def __(duckdb, filtered_orders, mo, px):
    _chart = None
    if not filtered_orders.empty:
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
        _chart = mo.ui.plotly(fig_region)
    _chart
    return


@app.cell
def __(duckdb, filtered_items, mo, px):
    _chart = None
    if not filtered_items.empty:
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
        _chart = mo.ui.plotly(fig_cat)
    _chart
    return


@app.cell
def __(duckdb, filtered_orders, go, mo):
    _chart = None
    if not filtered_orders.empty:
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
        _chart = mo.ui.plotly(fig_pay)
    _chart
    return


@app.cell
def __(duckdb, filtered_orders, mo, px):
    _chart = None
    if not filtered_orders.empty:
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
        _chart = mo.ui.plotly(fig_review)
    _chart
    return


@app.cell
def __(duckdb, filtered_orders, mo, px):
    _chart = None
    if not filtered_orders.empty:
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
        _chart = mo.ui.plotly(fig_del)
    _chart
    return


@app.cell
def __(mo):
    mo.md("---\n## 🔍 Data Explorer")
    return


@app.cell
def __(filtered_orders, mo):
    _output = mo.md("⚠️ No data to display")
    if not filtered_orders.empty:
        display_cols = ['order_id', 'order_status', 'customer_region', 'total_order_value', 
                        'payment_type', 'review_score', 'delivery_days', 'is_late', 'order_month']
        available_cols = [c for c in display_cols if c in filtered_orders.columns]
        _output = mo.ui.dataframe(filtered_orders[available_cols].head(100))
    _output
    return


@app.cell
def __(mo):
    mo.md("---\n## 🏗️ Data Warehouse Architecture")
    return


@app.cell
def __(mo):
    mo.image(src="../docs/data_warehouse_architecture.png", alt="Olist Data Warehouse Architecture", rounded=True)
    return


@app.cell
def __(mo):
    mo.md(
        """
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
        """
    )
    return


if __name__ == "__main__":
    app.run()
