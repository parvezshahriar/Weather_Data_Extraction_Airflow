from datetime import datetime
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task


# LATITUDE AND LONGITUDE FOR DHAKA
LATITUDE = "23.6850"
LONGITUDE = "90.3563"
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
}

with DAG(
    dag_id='etl_weather',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['weather', 'etl'],
) as dag:

    @task()
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        create_table_query = """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude VARCHAR(20),
                longitude VARCHAR(20),
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                time TIMESTAMP PRIMARY KEY
            );
        """
        postgres_hook.run(create_table_query)

    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'time': current_weather['time']
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        insert_query = """
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode, time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                windspeed = EXCLUDED.windspeed,
                winddirection = EXCLUDED.winddirection,
                weathercode = EXCLUDED.weathercode;
        """
        postgres_hook.run(insert_query, parameters=(
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode'],
            transformed_data['time']
        ))

    # Task execution flow
    create_table_step = create_table()
    raw_weather = extract_weather_data()
    transformed_weather = transform_weather_data(raw_weather)
    load_weather = load_weather_data(transformed_weather)

    create_table_step >> raw_weather >> transformed_weather >> load_weather