from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.models import Connection
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import pandas as pd

polygon_key = Variable.get("polygon_key")
postgres_conn_id = "my_postgres_conn"


def request_data():
    try:
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        date = yesterday.strftime("%Y-%m-%d")

        response = requests.get(
            f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&apiKey={polygon_key}"
        )

        result = response.json()

        return result["results"]
    except Exception as e:
        raise SystemError(e)
    return None


def insert_data():
    try:
        postgres_conn_id = "my_postgres_conn"
        conn = Connection.get_connection_from_secrets(postgres_conn_id)
        conn_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        conn = psycopg2.connect(conn_uri)
        print("Conexão bem-sucedida!")

        data = request_data()

        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stocks (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10),
                trading_volume FLOAT,
                volume_weighted_average DECIMAL(12, 4),
                open_price FLOAT,
                close_price FLOAT,
                highest_price FLOAT,
                lowest_price FLOAT,
                window_end_timestamp BIGINT,
                number_transactions INT,
                created_at TIMESTAMP
            )
            """
        )

        for item in data:
            required_keys = ["T", "v", "vw", "o", "c", "h", "l", "t", "n"]
            if all(key in item for key in required_keys):
                cur.execute(
                    """
                    INSERT INTO stocks (ticker, trading_volume, volume_weighted_average, open_price, close_price, highest_price, lowest_price, window_end_timestamp, number_transactions, created_at ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    (
                        item["T"],
                        item["v"],
                        item["vw"],
                        item["o"],
                        item["c"],
                        item["h"],
                        item["l"],
                        item["t"],
                        item["n"],
                    ),
                )
            else:
                print("Dados incompletos para inserção:", item)

        conn.commit()

        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print("Erro ao conectar ao banco de dados:", e)


default_args = {
    "owner": "Roberto Sousa",
    "start_date": datetime(2024, 3, 21),
    "retries": 1,
}

dag = DAG(
    "stocks_dag",
    default_args=default_args,
    description="Uma DAG simples que executa um comando Python.",
    schedule="@daily",
)

wait_for_service = HttpSensor(
    task_id="wait_for_service",
    http_conn_id="polygon_http_conn",
    endpoint="/system",
    request_params={},
    response_check=lambda response: response.status_code == 200,
    poke_interval=60,
    timeout=200,
    dag=dag,
)

wait_for_postgres = SqlSensor(
    task_id="wait_for_postgres",
    conn_id="my_postgres_conn",
    sql="SELECT 1",
    timeout=200,
    poke_interval=60,
    mode="poke",
    dag=dag,
)

save_data = PythonOperator(
    task_id="task_2",
    python_callable=insert_data,
    dag=dag,
)

wait_for_service >> wait_for_postgres >> save_data
