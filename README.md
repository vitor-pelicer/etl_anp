# etl_anp

## Pipeline de dados ETL
O projeto é uma DAG para ser executada no Apache Airflow. A dag segue o modelo ETL (Extract Transform Load).
A dag faz a leitura e transformação de dados públicos da Agência Nacional do Petróleo (ANP). O órgão disponibiliza os dados em formato CSV. A DAG faz a leitura de todos os arquivos da pasta, transforma os dados e os envia para um banco de dados Postgresql. Depois de lidos os arquivos são movidos para um outro diretório, o que evita que um arquivo seja lido mais de uma vez por engano.
