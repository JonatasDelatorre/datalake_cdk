import awswrangler as wr
import pandas as pd
from datetime import datetime

# today = datetime.today()
# year = today.year
# month = today.month
# day = today.day

YEAR = '2025'
MONTH = '01'
DAY = '30'

SOURCE_BUCKET = 's3://health-datalake-dev-raw-ACCOUNT_ID'
DESTINATION_BUCKET = 's3://health-datalake-dev-cleaned-ACCOUNT_ID'

def process_health_data(source_bucket, destination_bucket, year, month, day):
    source_data_path = f'HealthAutoExport-{year}-{month}-{day}-{year}-{month}-{day}.csv'
    source_path = f'{source_bucket}/{source_data_path}'

    destination_data_path = 'HealthAutoExport/'
    destination_path = f'{destination_bucket}/{destination_data_path}'

    df = wr.s3.read_csv(source_path, index_col=False)

    df = df[[
        "Date",
        "Dietary Energy (kJ)",
        "Resting Energy (kJ)",
        "Active Energy (kJ)",
        "Heart Rate [Max] (bpm)",
        "Heart Rate [Min] (bpm)",
        "Walking + Running Distance (km)",
        "Walking Speed (km/hr)",
        "Apple Stand Hour (hours)"
    ]]

    # Remover caracteres especiais
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)

    # Trocar espaços por _ 
    df.columns = df.columns.str.replace(' ', '_')

    # Colocar tudo em minúsculas
    df.columns = df.columns.str.lower()

    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day

    df.head()

    wr.s3.to_parquet(df=df, 
        path=destination_path, 
        dataset=True, 
        mode='append',
        partition_cols=['year', 'month', 'day']
    )

def process_workout_data(source_bucket, destination_bucket, year, month, day):
    source_data_path = f'Workouts-{year}{month}{day}_000000-{year}{month}{day}_235959.csv'
    source_path = f'{source_bucket}/{source_data_path}'

    destination_data_path = 'Workouts/'
    destination_path = f'{destination_bucket}/{destination_data_path}'

    df = wr.s3.read_csv(source_path, index_col=False)

    df = df[[
        'Workout Type',
        'Start',
        'Duration',
        'Active Energy (kcal)',
        'Intensity (kcal/hr·kg)',
        'Max. Heart Rate (bpm)',
        'Avg. Heart Rate (bpm)',
        'Step Count',
        'Distance (km)'
    ]]

    # Remover caracteres especiais
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)

    # Trocar espaços por _ 
    df.columns = df.columns.str.replace(' ', '_')

    # Colocar tudo em minúsculas
    df.columns = df.columns.str.lower()

    df['start'] = pd.to_datetime(df['start'])
    df['year'] = df['start'].dt.year
    df['month'] = df['start'].dt.month
    df['day'] = df['start'].dt.day

    wr.s3.to_parquet(df=df, 
        path=destination_path, 
        dataset=True, 
        mode='append',
        partition_cols=['year', 'month', 'day']
    )

def handler(event, context):
    print(event)

    process_health_data(SOURCE_BUCKET, DESTINATION_BUCKET, YEAR, MONTH, DAY)

    process_workout_data(SOURCE_BUCKET, DESTINATION_BUCKET, YEAR, MONTH, DAY)
    
    return {"Status": "OK"}
