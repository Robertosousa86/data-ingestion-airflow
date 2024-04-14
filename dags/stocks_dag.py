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


def create_connection(postgres_conn_id):
    try:
        conn = Connection.get_connection_from_secrets(postgres_conn_id)
        conn_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        conn = psycopg2.connect(conn_uri)
        return conn
    except psycopg2.Error as e:
        raise SystemExit(e)


conn = create_connection(postgres_conn_id)


def get_data(polygon_key):
    try:
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        date = yesterday.strftime("%Y-%m-%d")

        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&apiKey={polygon_key}"

        response = requests.get(url)

        result = response.json()

        return result["results"]
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


data = get_data(polygon_key)


def create_dataframe(data):

    df = pd.DataFrame(data)

    df.fillna(0, inplace=True)

    df["t"] = pd.to_datetime(df["t"], unit="ms")

    df.rename(
        columns={
            "T": "ticker",
            "v": "trading_volume",
            "vw": "volume_weighted_average",
            "o": "open_price",
            "c": "close_price",
            "h": "highest_price",
            "l": "lowest_price",
            "t": "window_end_timestamp",
            "n": "number_of_transactions",
        },
        inplace=True,
    )

    df["number_of_transactions"] = df["number_of_transactions"].astype(int)

    return df


df = create_dataframe(data)


def insert_data(df, conn):
    try:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO stocks (
                    ticker, trading_volume, volume_weighted_average, 
                    open_price, close_price, highest_price, lowest_price, 
                    window_end_timestamp, number_of_transactions, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                )
            """,
                (
                    row["ticker"],
                    row["trading_volume"],
                    row["volume_weighted_average"],
                    row["open_price"],
                    row["close_price"],
                    row["highest_price"],
                    row["lowest_price"],
                    row["window_end_timestamp"],
                    row["number_of_transactions"],
                ),
            )
        conn.commit()
        cursor.close()
        conn.close()
        print("Dados inseridos com sucesso!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Erro ao inserir dados:", error)


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


simple_moving_average = caculate_smv(df)
relative_strength_index = calculate_rsi(df)
upper_bollinger_band, lower_bollinger_band = calculate_bollinger_bands(df)


def create_indicator_dataframe():
    indicators_dataframe = pd.DataFrame(
        {
            "ticker": df["ticker"],
            "window_end_timestamp": df["window_end_timestamp"],
            "simple_moving_average": simple_moving_average,
            "relative_strength_index": relative_strength_index,
            "upper_bollinger_band": upper_bollinger_band,
            "lower_bollinger_band": lower_bollinger_band,
        }
    )

    indicators_dataframe.fillna(0, inplace=True)

    return indicators_dataframe


indicators_df = create_indicator_dataframe()
print(indicators_df)


def insert_indicators(indicators_df, conn):
    try:
        cursor = conn.cursor()
        for index, row in indicators_df.iterrows():
            cursor.execute("SELECT id FROM stocks WHERE ticker = %s", (row["ticker"],))
            stock_id = cursor.fetchone()[
                0
            ]  # Supondo que o ID seja a primeira coluna retornada

            cursor.execute(
                """
                INSERT INTO indicators (
                    stock_id, ticker, window_end_timestamp, simple_moving_average, relative_strength_index, 
                    upper_bollinger_band, lower_bollinger_band, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                )
                """,
                (
                    stock_id,
                    row["ticker"],
                    row["window_end_timestamp"],
                    row["simple_moving_average"],
                    row["relative_strength_index"],
                    row["upper_bollinger_band"],
                    row["lower_bollinger_band"],
                ),
            )
        conn.commit()
        cursor.close()
        print("Dados inseridos com sucesso!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Erro ao inserir dados:", error)


default_args = {
    "owner": "Roberto Sousa",
    "start_date": datetime(2024, 4, 11),
    "retries": 1,
}

dag = DAG(
    "stocks_dag",
    default_args=default_args,
    description="Uma DAG para agendamento, requisição, limpeza e carregamento de dados da API Polygon",
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
    op_kwargs={"df": df, "conn": conn},
    dag=dag,
)

save_indicators = PythonOperator(
    task_id="insert_indicators",
    python_callable=insert_indicators,
    op_kwargs={"indicators_df": indicators_df, "conn": conn},
    dag=dag,
)


wait_for_service >> wait_for_postgres >> save_data >> save_indicators
