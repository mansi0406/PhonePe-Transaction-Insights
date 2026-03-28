
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, text
import plotly.express as px

# Database connection
DB_USER = 'postgres'
DB_PASSWORD = 'mansi0406'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'phonepe_pulse'

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def run_queries():
    print("--- SQL Data Analysis ---")
    
    with engine.connect() as conn:
        # 1. Total transaction amount and count per state
        query1 = text("""
        SELECT state, SUM(transaction_count) as total_count, SUM(transaction_amount) as total_amount
        FROM aggregated_transaction
        GROUP BY state
        ORDER BY total_amount DESC
        LIMIT 10;
        """)
        df1 = pd.read_sql(query1, conn)
        print("\nTop 10 States by Transaction Amount:")
        print(df1)
        
        # 2. Transaction types distribution
        query2 = text("""
        SELECT transaction_type, SUM(transaction_count) as total_count, SUM(transaction_amount) as total_amount
        FROM aggregated_transaction
        GROUP BY transaction_type
        ORDER BY total_amount DESC;
        """)
        df2 = pd.read_sql(query2, conn)
        print("\nTransaction Types Distribution:")
        print(df2)
        
        # 3. Top 10 brands used by users
        query3 = text("""
        SELECT brand, SUM(count) as total_count
        FROM aggregated_user
        WHERE brand IS NOT NULL
        GROUP BY brand
        ORDER BY total_count DESC
        LIMIT 10;
        """)
        df3 = pd.read_sql(query3, conn)
        print("\nTop 10 Mobile Brands by Users:")
        print(df3)
        
        # 4. Registered Users and App Opens over years
        query4 = text("""
        SELECT year, SUM(registered_users) as total_users, SUM(app_opens) as total_opens
        FROM aggregated_user
        GROUP BY year
        ORDER BY year;
        """)
        df4 = pd.read_sql(query4, conn)
        print("\nYearly User Growth:")
        print(df4)

        # 5. Top districts by transaction count
        query5 = text("""
        SELECT district, SUM(count) as total_count
        FROM map_transaction
        GROUP BY district
        ORDER BY total_count DESC
        LIMIT 10;
        """)
        df5 = pd.read_sql(query5, conn)
        print("\nTop 10 Districts by Transaction Count:")
        print(df5)

if __name__ == "__main__":
    run_queries()
