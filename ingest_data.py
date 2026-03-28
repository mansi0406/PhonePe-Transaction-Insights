
import os
import json
import pandas as pd
from sqlalchemy import create_engine

# Database connection
DB_USER = 'postgres'
DB_PASSWORD = 'mansi0406'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'phonepe_pulse'

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

DATA_PATH = r"c:\Users\HP\Labmentix Program\PhonePe\pulse-master\pulse-master\data"

def ingest_aggregated_transaction():
    path = os.path.join(DATA_PATH, "aggregated", "transaction", "country", "india", "state")
    data_list = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for entry in data['data']['transactionData']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'transaction_type': entry['name'],
                            'transaction_count': entry['paymentInstruments'][0]['count'],
                            'transaction_amount': entry['paymentInstruments'][0]['amount']
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('aggregated_transaction', engine, if_exists='replace', index=False)
    print("Ingested aggregated_transaction")

def ingest_aggregated_user():
    path = os.path.join(DATA_PATH, "aggregated", "user", "country", "india", "state")
    data_list = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    reg_users = data['data']['aggregated']['registeredUsers']
                    app_opens = data['data']['aggregated']['appOpens']
                    if data['data']['usersByDevice']:
                        for entry in data['data']['usersByDevice']:
                            data_list.append({
                                'state': state,
                                'year': int(year),
                                'quarter': int(file.split('.')[0]),
                                'brand': entry['brand'],
                                'count': entry['count'],
                                'percentage': entry['percentage'],
                                'registered_users': reg_users,
                                'app_opens': app_opens
                            })
                    else:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'brand': None,
                            'count': 0,
                            'percentage': 0,
                            'registered_users': reg_users,
                            'app_opens': app_opens
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('aggregated_user', engine, if_exists='replace', index=False)
    print("Ingested aggregated_user")

def ingest_aggregated_insurance():
    path = os.path.join(DATA_PATH, "aggregated", "insurance", "country", "india", "state")
    data_list = []
    if not os.path.exists(path): return
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for entry in data['data']['transactionData']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'insurance_type': entry['name'],
                            'insurance_count': entry['paymentInstruments'][0]['count'],
                            'insurance_amount': entry['paymentInstruments'][0]['amount']
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('aggregated_insurance', engine, if_exists='replace', index=False)
    print("Ingested aggregated_insurance")

def ingest_map_transaction():
    path = os.path.join(DATA_PATH, "map", "transaction", "hover", "country", "india", "state")
    data_list = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for entry in data['data']['hoverDataList']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'district': entry['name'],
                            'count': entry['metric'][0]['count'],
                            'amount': entry['metric'][0]['amount']
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('map_transaction', engine, if_exists='replace', index=False)
    print("Ingested map_transaction")

def ingest_map_user():
    path = os.path.join(DATA_PATH, "map", "user", "hover", "country", "india", "state")
    data_list = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for district, metrics in data['data']['hoverData'].items():
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'district': district,
                            'registered_users': metrics['registeredUsers'],
                            'app_opens': metrics['appOpens']
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('map_user', engine, if_exists='replace', index=False)
    print("Ingested map_user")

def ingest_map_insurance():
    path = os.path.join(DATA_PATH, "map", "insurance", "hover", "country", "india", "state")
    data_list = []
    if not os.path.exists(path): return
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for entry in data['data']['hoverDataList']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'district': entry['name'],
                            'count': entry['metric'][0]['count'],
                            'amount': entry['metric'][0]['amount']
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('map_insurance', engine, if_exists='replace', index=False)
    print("Ingested map_insurance")

def ingest_top_transaction():
    path = os.path.join(DATA_PATH, "top", "transaction", "country", "india", "state")
    data_list = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for district in data['data']['districts']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'entity_type': 'district',
                            'entity_name': district['entityName'],
                            'count': district['metric']['count'],
                            'amount': district['metric']['amount']
                        })
                    for pincode in data['data']['pincodes']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'entity_type': 'pincode',
                            'entity_name': pincode['entityName'],
                            'count': pincode['metric']['count'],
                            'amount': pincode['metric']['amount']
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('top_transaction', engine, if_exists='replace', index=False)
    print("Ingested top_transaction")

def ingest_top_user():
    path = os.path.join(DATA_PATH, "top", "user", "country", "india", "state")
    data_list = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for district in data['data']['districts']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'entity_type': 'district',
                            'entity_name': district['name'],
                            'registered_users': district['registeredUsers']
                        })
                    for pincode in data['data']['pincodes']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'entity_type': 'pincode',
                            'entity_name': pincode['name'],
                            'registered_users': pincode['registeredUsers']
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('top_user', engine, if_exists='replace', index=False)
    print("Ingested top_user")

def ingest_top_insurance():
    path = os.path.join(DATA_PATH, "top", "insurance", "country", "india", "state")
    data_list = []
    if not os.path.exists(path): return
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for district in data['data']['districts']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'entity_type': 'district',
                            'entity_name': district['entityName'],
                            'count': district['metric']['count'],
                            'amount': district['metric']['amount']
                        })
                    for pincode in data['data']['pincodes']:
                        data_list.append({
                            'state': state,
                            'year': int(year),
                            'quarter': int(file.split('.')[0]),
                            'entity_type': 'pincode',
                            'entity_name': pincode['entityName'],
                            'count': pincode['metric']['count'],
                            'amount': pincode['metric']['amount']
                        })
    df = pd.DataFrame(data_list)
    df.to_sql('top_insurance', engine, if_exists='replace', index=False)
    print("Ingested top_insurance")

if __name__ == "__main__":
    ingest_aggregated_transaction()
    ingest_aggregated_user()
    ingest_aggregated_insurance()
    ingest_map_transaction()
    ingest_map_user()
    ingest_map_insurance()
    ingest_top_transaction()
    ingest_top_user()
    ingest_top_insurance()
    print("All data ingested successfully!")
