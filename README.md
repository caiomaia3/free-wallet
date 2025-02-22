# free-wallet


Este repositório tem o objetivo de armazenar um conjunto de ferramentas que, trabalhando em conjunto, permitam que um dev/investidor consiga automatizar alguns processo de gestão de sua carteira de investimentos. Tarefas como extrair dados, organizar, transformar, armazenar, consolidar, salvar, gerar gráficos, visualizações, etc.

A Primeira feature a ser desenvolvida é um ETL que permita salvar em um banco de dados postgre sql rodando localmente dados vindos de um arquivo xlsx extraido do site da B3.

A princípio as ferramentas que serão utilizadas serão:

- Apache Airflow
- Docker
- PostgreSql

Um fluxo do processos desta feature pode ser visto em:
[diagram](./diagrams/free-wallet.drawio)