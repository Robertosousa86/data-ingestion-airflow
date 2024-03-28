from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.models import Connection
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import sqlalchemy
import requests

polygon_key = Variable.get("polygon_key")
postgres_conn_id = "my_postgres_conn"


def request_data():
    try:
        today = datetime.now()
        yesterday = today - timedelta(days=7)
        date = yesterday.strftime("%Y-%m-%d")

        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&apiKey={polygon_key}"

        response = requests.get(url)

        result = response.json()

        return result["results"]
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


def create_connection():
    try:
        postgres_conn_id = "my_postgres_conn"
        conn = Connection.get_connection_from_secrets(postgres_conn_id)
        conn_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        conn = psycopg2.connect(conn_uri)
        return conn
    except psycopg2.Error as e:
        raise SystemExit(e)


def insert_data():
    try:
        data = request_data()
        conn = create_connection()
        cur = conn.cursor()

        for item in data:
            required_keys = ["T", "v", "vw", "o", "c", "h", "l", "t", "n"]
            if all(key in item for key in required_keys):
                cur.execute(
                    """
                    INSERT INTO stocks (ticker, trading_volume, volume_weighted_average,
                    open_price, close_price, highest_price, lowest_price, window_end_timestamp,
                    number_transactions, created_at ) 
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
        raise SystemExit(e)


def create_dataframe():
    conn = create_connection()

    query = """ SELECT ticker, close_price, window_end_timestamp FROM stocks"""

    df = pd.read_sql_query(query, conn)

    return df


def caculate_smv(df, window=20):
    simple_moving_average = df["close_price"].rolling(window).mean()

    return simple_moving_average


def calculate_rsi(df, periods=14, ema=True):
    close_delta = df["close_price"].diff()

    up = close_delta.clip(lower=0)
    down = -1 * close_delta.clip(upper=0)

    if ema == True:
        ma_up = up.ewm(com=periods - 1, adjust=True, min_periods=periods).mean()
        ma_down = down.ewm(com=periods - 1, adjust=True, min_periods=periods).mean()
    else:
        ma_up = up.rolling(window=periods, adjust=False).mean()
        ma_down = down.rolling(window=periods, adjust=False).mean()

    relative_strength_index = ma_up / ma_down
    relative_strength_index = 100 - (100 / (1 + relative_strength_index))

    return relative_strength_index


def calculate_bollinger_bands(df, window=20, num_std=2):

    rolling_mean = df["close_price"].rolling(window=window).mean()

    rolling_std = df["close_price"].rolling(window=window).std()

    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)

    return upper_band, lower_band


def add_indicators_to_dataframe():
    df = create_dataframe()

    df["SMV"] = caculate_smv(df)

    df["RSI"] = calculate_rsi(df)

    upper_band, lower_band = calculate_bollinger_bands(df)
    df["Upper_Band"] = upper_band
    df["Lower_Band"] = lower_band

    return df


def indicators_to_database():
    conn = create_connection()
    cursor = conn.cursor()

    new_df = add_indicators_to_dataframe()

    for index, row in new_df.iterrows():
        cursor.execute(
            """
            INSERT INTO stocks_indicators (ticker, window_end_timestamp, simple_moving_average, 
            relative_strength_index, upper_bollinger_band, lower_bollinger_band, calculated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
            (
                row["ticker"],
                row["window_end_timestamp"],
                row["SMV"],
                row["RSI"],
                row["Upper_Band"],
                row["Lower_Band"],
                datetime.now(),
            ),
        )

    conn.commit()
    cursor.close()


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
    task_id="insert_data",
    python_callable=insert_data,
    dag=dag,
)

add_indicators = PythonOperator(
    task_id="insert_indicators",
    python_callable=indicators_to_database,
    dag=dag,
)

wait_for_service >> wait_for_postgres >> save_data >> add_indicators
