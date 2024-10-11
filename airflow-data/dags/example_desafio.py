from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import csv
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##


# Função para exportar os dados da tabela 'Order' para CSV
def export_orders_to_csv():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    cursor = conn.cursor()
    query = "SELECT * FROM 'Order'"
    cursor.execute(query)
    orders = cursor.fetchall()

    with open('output_orders.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in cursor.description])  # Escreve os headers
        writer.writerows(orders)
    
    conn.close()

# Função para calcular a soma da quantidade vendida para o Rio de Janeiro
def calculate_order_count():
    conn = sqlite3.connect('data/Northwind_small.sqlite')

    # Lê o arquivo CSV exportado na primeira task
    orders_df = pd.read_csv('output_orders.csv')

    # Lê a tabela OrderDetail
    order_details_df = pd.read_sql_query("SELECT * FROM 'OrderDetail'", conn)

    # Faz um JOIN entre as tabelas pelo 'OrderId' da OrderDetail e o 'Id' da Order
    merged_df = pd.merge(orders_df, order_details_df, left_on='Id', right_on='OrderId')

    # Filtra pelo ShipCity "Rio de Janeiro"
    rio_orders = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']

    # Calcula a soma da quantidade
    total_quantity = rio_orders['Quantity'].sum()

    # Salva a contagem no arquivo count.txt
    with open('count.txt', 'w') as f:
        f.write(str(total_quantity))

    conn.close()


# Definição do DAG
with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Exportar os dados de 'Order' para CSV
    export_orders = PythonOperator(
        task_id='export_orders_to_csv',
        python_callable=export_orders_to_csv
    )

    # Task 2: Calcular a quantidade de pedidos para Rio de Janeiro
    calculate_orders = PythonOperator(
        task_id='calculate_order_count',
        python_callable=calculate_order_count
    )

    # Task final: Exportar o arquivo codificado final_output.txt
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
    )

    # Definição da sequência de execução
    export_orders >> calculate_orders >> export_final_output
