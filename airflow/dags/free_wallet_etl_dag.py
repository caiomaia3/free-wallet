from datetime import datetime, timedelta
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def convert_string_to_datetime(date_string):
  """Converts a string in the format 'dd/mm/yyyy' to a datetime object.

  Args:
    date_string: The date string to convert.

  Returns:
    A datetime object representing the date, or None if the input is invalid.
  """
  try:
    return datetime.strptime(date_string, '%d/%m/%Y')
  except ValueError:
    return None

def get_product_name(product):
  splitted_product = product.split(' - ')
  if(len(splitted_product)==0): return None
  else:
    return ' - '.join(splitted_product[1:])


def convert_to_float(input_value):
  """Converts the input to a float if possible, otherwise returns None.

  Args:
    input_value: The value to convert.

  Returns:
    The float representation of the input if successful, None otherwise.
  """
  try:
    return float(input_value)
  except (ValueError, TypeError):
    return None

def create_or_get_container(container_name,blob_service_client):
  """Creates a container if it doesn't exist, otherwise gets an existing one.

  Args:
    container_name: The name of the container.

  Returns:
    The container client object.
  """
  try:
      container_client = blob_service_client.create_container(container_name)
      print(f"Container '{container_name}' created successfully.")
      return container_client
  except Exception as e:
      if "ContainerAlreadyExists" in str(e):
          print(f"Container '{container_name}' already exists. Getting the existing container.")
          container_client = blob_service_client.get_container_client(container_name)
          return container_client
      else:
          print(f"Error creating or getting container: {e}")
          return None






def fetch_xlsx():
    connection_string = os.getenv('_AZ_CONN')
    if connection_string:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
        if(blob_service_client): print("Serviço criado com sucesso!")

        testfiles_container = blob_service_client.get_container_client("testfiles") ## transformar em variável
        blobs = testfiles_container.list_blobs()
        movimentacao_xlsx = []
        for blob in blobs:
            if(blob.name.startswith("movimentacao") and blob.name.endswith(".xlsx")):
                movimentacao_xlsx.append(blob.name)

        if(len(movimentacao_xlsx) > 0):
            print(movimentacao_xlsx[0])
            blob_client = blob_service_client.get_blob_client(container="testfiles", blob=movimentacao_xlsx[0])
            download_stream = blob_client.download_blob()
            df = pd.read_excel(download_stream.readall())
            print(df.head())

            rename = {'Entrada/Saída':'transaction_direction','Data':'transaction_date','Movimentação':'transaction_type','Produto': 'product','Instituição':'financial_institution','Quantidade':'quantity', 'Preço unitário':'unit_price', 'Valor da Operação':'transaction_value' }

            df1 = df.rename(columns=rename)

            df1['transaction_direction'] = df1['transaction_direction'].map({'Debito':'output','Débito':'output','Credito':'input','Crédito':'input'})

            df1['transaction_date'] = df1['transaction_date'].apply(lambda x: convert_string_to_datetime(x))

            df1['is_yield'] = df1['transaction_type'] == 'Rendimento'

            df1['product_code'] = df1['product'].apply(lambda x: x.split(' - ')[0])
            df1['product_name'] = df1['product'].apply(lambda x: get_product_name(x))

            df1['unit_price'] = df1['unit_price'].apply(lambda x: convert_to_float(x))
            df1['transaction_value'] = df1['transaction_value'].apply(lambda x: convert_to_float(x))

            # Specify the name of the container to store the parsed CSV file
            container_name = "parsed-data"

            # Create or get the specified container
            container_client = create_or_get_container(container_name,blob_service_client)

            if container_client:
            # Save the DataFrame as a CSV file in the specified container
                csv_file_name = movimentacao_xlsx[0].split('.')[0] + ".csv" # Choose a name for the CSV file.

                try:
                    # Convert the DataFrame to CSV in memory
                    csv_data = df1.to_csv(index=False) # index=False to avoid writing the row index

                    # Upload the CSV data to the blob
                    blob_client = container_client.get_blob_client(blob=csv_file_name)
                    blob_client.upload_blob(csv_data, overwrite=True) # overwrite=True to replace the file if it exists

                    print(f"DataFrame successfully saved to blob storage as '{csv_file_name}' in container '{container_name}'.")

                except Exception as e:
                    print(f"Error uploading the CSV file to blob storage: {e}")

default_args = {
    "depends_on_past": False,
    "email": ["caiomaia3@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    'free_wallet_etl_dag',
    default_args=default_args,
    description='The Free Wallet pipeline that extracts B3 report, transform data and save for using in Dica de Hoje spreadsheet',
    schedule_interval=timedelta(days=1),
    start_date=datetime.now() + timedelta(days=-2),
    catchup=False,
    tags=['dev'],
) as dag:
    fetch_xlsx_task = PythonOperator(
        task_id="fetch_xlsx_task",
        python_callable=fetch_xlsx,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )