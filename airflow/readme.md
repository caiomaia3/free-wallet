# Airflow Running in Docker


1.  Vá até o site da documentação do airflow e pegue o link para baixar o arquivo de docker compose. [linki](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

Para o momento que eu estou escrevendo este é o comando:
```shell
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'
```

Mude a configuraçào do Executer para LocalExecuter ao invés do CeleryExecuter e retire todas as configuracões relacionadas ao Celery, Redis e Flower.


Para finalizar a configuração é necessário criar o folders necessários e habilitar o Id do usuário para o Airflow


```shell
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```


Agora será necessário executar as migrations para criação do banco de dados e criar os usuários.
Para isto basta executar:

```shell
docker compose up airflow-init

```


[reference](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)