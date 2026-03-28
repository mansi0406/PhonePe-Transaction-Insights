
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px

# Database connection
DB_USER = 'postgres'
DB_PASSWORD = 'mansi0406'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'phonepe_pulse'

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

st.set_page_config(page_title="PhonePe Pulse Analysis", layout="wide")
st.title("📱 PhonePe Pulse - Data Visualization & Analysis")

# Sidebar for filters
st.sidebar.header("Filters")
with engine.connect() as conn:
    years = pd.read_sql(text("SELECT DISTINCT year FROM aggregated_transaction ORDER BY year"), conn)['year'].tolist()
    states = pd.read_sql(text("SELECT DISTINCT state FROM aggregated_transaction ORDER BY state"), conn)['state'].tolist()

selected_year = st.sidebar.selectbox("Select Year", years)
selected_quarter = st.sidebar.selectbox("Select Quarter", [1, 2, 3, 4])
selected_state = st.sidebar.selectbox("Select State", ["All"] + states)

# Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Transaction Analysis", "User Analysis", "Insurance Analysis", "Top Charts", "Trend Analysis"])

with tab1:
    st.header("Transaction Analysis")
    with engine.connect() as conn:
        if selected_state == "All":
            query = text(f"SELECT state, SUM(transaction_count) as count, SUM(transaction_amount) as amount FROM aggregated_transaction WHERE year={selected_year} AND quarter={selected_quarter} GROUP BY state ORDER BY amount DESC")
        else:
            query = text(f"SELECT transaction_type, SUM(transaction_count) as count, SUM(transaction_amount) as amount FROM aggregated_transaction WHERE year={selected_year} AND quarter={selected_quarter} AND state='{selected_state}' GROUP BY transaction_type ORDER BY amount DESC")
        
        df = pd.read_sql(query, conn)
        
        col1, col2 = st.columns(2)
        with col1:
            if selected_state == "All":
                fig = px.bar(df, x='state', y='amount', title="Transaction Amount by State", color='amount', color_continuous_scale='Viridis')
            else:
                fig = px.pie(df, names='transaction_type', values='amount', title=f"Transaction Type Distribution in {selected_state}")
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            if selected_state == "All":
                fig2 = px.bar(df, x='state', y='count', title="Transaction Count by State", color='count', color_continuous_scale='Magma')
            else:
                fig2 = px.bar(df, x='transaction_type', y='count', title=f"Transaction Count by Type in {selected_state}", color='count')
            st.plotly_chart(fig2, use_container_width=True)

with tab2:
    st.header("User Analysis")
    with engine.connect() as conn:
        if selected_state == "All":
            query = text(f"SELECT state, MAX(registered_users) as users FROM aggregated_user WHERE year={selected_year} AND quarter={selected_quarter} GROUP BY state ORDER BY users DESC")
            df_u = pd.read_sql(query, conn)
            fig_u = px.bar(df_u, x='state', y='users', title="Registered Users by State", color='users', color_continuous_scale='Cividis')
            st.plotly_chart(fig_u, use_container_width=True)
        else:
            col1, col2 = st.columns(2)
            with col1:
                query = text(f"SELECT brand, SUM(count) as count FROM aggregated_user WHERE year={selected_year} AND quarter={selected_quarter} AND state='{selected_state}' GROUP BY brand ORDER BY count DESC")
                df_u = pd.read_sql(query, conn)
                fig_u = px.pie(df_u, names='brand', values='count', title=f"Device Brands in {selected_state}")
                st.plotly_chart(fig_u, use_container_width=True)
            with col2:
                query_u = text(f"SELECT registered_users, app_opens FROM aggregated_user WHERE year={selected_year} AND quarter={selected_quarter} AND state='{selected_state}' LIMIT 1")
                df_u_metrics = pd.read_sql(query_u, conn)
                if not df_u_metrics.empty:
                    st.metric("Total Registered Users", f"{df_u_metrics['registered_users'].iloc[0]:,}")
                    st.metric("Total App Opens", f"{df_u_metrics['app_opens'].iloc[0]:,}")

with tab3:
    st.header("Insurance Analysis")
    with engine.connect() as conn:
        try:
            if selected_state == "All":
                query = text(f"SELECT state, SUM(insurance_count) as count, SUM(insurance_amount) as amount FROM aggregated_insurance WHERE year={selected_year} AND quarter={selected_quarter} GROUP BY state ORDER BY amount DESC")
            else:
                query = text(f"SELECT insurance_type, SUM(insurance_count) as count, SUM(insurance_amount) as amount FROM aggregated_insurance WHERE year={selected_year} AND quarter={selected_quarter} AND state='{selected_state}' GROUP BY insurance_type ORDER BY amount DESC")
            
            df_i = pd.read_sql(query, conn)
            if not df_i.empty:
                col1, col2 = st.columns(2)
                with col1:
                    if selected_state == "All":
                        fig_i = px.bar(df_i, x='state', y='amount', title="Insurance Amount by State", color='amount')
                    else:
                        fig_i = px.pie(df_i, names='insurance_type', values='amount', title=f"Insurance Type Distribution in {selected_state}")
                    st.plotly_chart(fig_i, use_container_width=True)
                with col2:
                    if selected_state == "All":
                        fig_i2 = px.bar(df_i, x='state', y='count', title="Insurance Count by State", color='count')
                    else:
                        fig_i2 = px.bar(df_i, x='insurance_type', y='count', title=f"Insurance Count by Type in {selected_state}", color='count')
                    st.plotly_chart(fig_i2, use_container_width=True)
            else:
                st.info("No insurance data available for the selected period.")
        except Exception as e:
            st.error(f"Error fetching insurance data: {e}")

with tab4:
    st.header("Top Performers")
    top_type = st.radio("Top 10 by:", ["Districts", "Pincodes"], horizontal=True)
    with engine.connect() as conn:
        entity = 'district' if top_type == "Districts" else 'pincode'
        if selected_state == "All":
            query = text(f"SELECT entity_name, SUM(count) as count, SUM(amount) as amount FROM top_transaction WHERE year={selected_year} AND quarter={selected_quarter} AND entity_type='{entity}' GROUP BY entity_name ORDER BY amount DESC LIMIT 10")
        else:
            query = text(f"SELECT entity_name, SUM(count) as count, SUM(amount) as amount FROM top_transaction WHERE year={selected_year} AND quarter={selected_quarter} AND state='{selected_state}' AND entity_type='{entity}' GROUP BY entity_name ORDER BY amount DESC LIMIT 10")
        
        df_top = pd.read_sql(query, conn)
        st.subheader(f"Top 10 {top_type} by Transaction Amount")
        fig_top = px.bar(df_top, x='entity_name', y='amount', color='amount', labels={'entity_name': top_type, 'amount': 'Total Amount'}, color_continuous_scale='Plasma')
        st.plotly_chart(fig_top, use_container_width=True)

with tab5:
    st.header("Trend Analysis (Over Years)")
    with engine.connect() as conn:
        if selected_state == "All":
            query_t = text("SELECT year, SUM(transaction_amount) as amount, SUM(transaction_count) as count FROM aggregated_transaction GROUP BY year ORDER BY year")
            query_u_t = text("SELECT year, SUM(registered_users) as users, SUM(app_opens) as opens FROM aggregated_user GROUP BY year ORDER BY year")
        else:
            query_t = text(f"SELECT year, SUM(transaction_amount) as amount, SUM(transaction_count) as count FROM aggregated_transaction WHERE state='{selected_state}' GROUP BY year ORDER BY year")
            query_u_t = text(f"SELECT year, SUM(registered_users) as users, SUM(app_opens) as opens FROM aggregated_user WHERE state='{selected_state}' GROUP BY year ORDER BY year")
        
        df_trend = pd.read_sql(query_t, conn)
        df_u_trend = pd.read_sql(query_u_t, conn)
        
        col1, col2 = st.columns(2)
        with col1:
            fig_t1 = px.line(df_trend, x='year', y='amount', title="Transaction Amount Trend", markers=True)
            st.plotly_chart(fig_t1, use_container_width=True)
            fig_t2 = px.line(df_u_trend, x='year', y='users', title="Registered Users Trend", markers=True)
            st.plotly_chart(fig_t2, use_container_width=True)
        with col2:
            fig_t3 = px.line(df_trend, x='year', y='count', title="Transaction Count Trend", markers=True)
            st.plotly_chart(fig_t3, use_container_width=True)
            fig_t4 = px.line(df_u_trend, x='year', y='opens', title="App Opens Trend", markers=True)
            st.plotly_chart(fig_t4, use_container_width=True)
