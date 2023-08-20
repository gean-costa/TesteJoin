# Databricks notebook source
# Definindo informações de conexão para um banco de dados PostgreSQL.
# Aqui, estamos configurando os detalhes do driver JDBC, credenciais do banco de dados
# e a URL de conexão necessária para acessar o banco de dados.

# Driver JDBC para PostgreSQL
driver = "org.postgresql.Driver"

# Credenciais do banco de dados
database_host = "psql-mock-database-cloud.postgres.database.azure.com"
database_port = "5432"
database_name = "ecom1692302642986jdgfoiqqgnwhjgtz"
user = "bktdrkjovneylsygcyjfmgsp@psql-mock-database-cloud"
password = "vokkfcasyhwkvhytorkpjimf"

# URL de conexão construída usando as informações acima
url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"


# COMMAND ----------

def get_remote_table(driver: str, url: str, table: str, user: str, password: str) -> DataFrame:
    """
    Lê uma tabela remota utilizando Spark DataFrame API.

    Args:
        driver (str): O driver JDBC para o banco de dados.
        url (str): A URL do banco de dados.
        table (str): O nome da tabela a ser lida.
        user (str): O nome de usuário para autenticação.
        password (str): A senha para autenticação.

    Returns:
        DataFrame: Um DataFrame contendo os dados da tabela remota.
    """
    
    remote_table = (spark.read
                    .format("jdbc")
                    .option("driver", driver)
                    .option("url", url)
                    .option("dbtable", table)
                    .option("user", user)
                    .option("password", password)
                    .load()
                    )
    
    return remote_table


# COMMAND ----------

# Obtendo dados de tabelas remotas do banco de dados utilizando a função get_remote_table.
# Aqui, estamos buscando dados de várias tabelas usando as credenciais e informações de conexão
# previamente configuradas. Cada chamada da função get_remote_table resulta em um DataFrame contendo
# os dados da tabela correspondente.

# Obtendo dados da tabela 'customers'
customers = get_remote_table(driver=driver, url=url, table='customers', user=user, password=password)

# Obtendo dados da tabela 'employees'
employees = get_remote_table(driver=driver, url=url, table='employees', user=user, password=password)

# Obtendo dados da tabela 'offices'
offices = get_remote_table(driver=driver, url=url, table='offices', user=user, password=password)

# Obtendo dados da tabela 'orderdetails'
orderdetails = get_remote_table(driver=driver, url=url, table='orderdetails', user=user, password=password)

# Obtendo dados da tabela 'orders'
orders = get_remote_table(driver=driver, url=url, table='orders', user=user, password=password)

# Obtendo dados da tabela 'payments'
payments = get_remote_table(driver=driver, url=url, table='payments', user=user, password=password)

# Obtendo dados da tabela 'product_lines'
product_lines = get_remote_table(driver=driver, url=url, table='product_lines', user=user, password=password)

# Obtendo dados da tabela 'products'
products = get_remote_table(driver=driver, url=url, table='products', user=user, password=password)


# COMMAND ----------

product_lines.display()

# COMMAND ----------

# Salvando DataFrames em formato Parquet.
# Neste trecho de código, estamos salvando os DataFrames resultantes das consultas
# às tabelas remotas em formato Parquet. Cada DataFrame é salvo em um local específico
# usando o método write.parquet, facilitando o armazenamento dos dados em um formato eficiente.

# Salvando DataFrame 'customers' em Parquet
customers.write.parquet("FileStore/TesteJoin/customers")

# Salvando DataFrame 'employees' em Parquet
employees.write.parquet("FileStore/TesteJoin/employees")

# Salvando DataFrame 'offices' em Parquet
offices.write.parquet("FileStore/TesteJoin/offices")

# Salvando DataFrame 'orderdetails' em Parquet
orderdetails.write.parquet("FileStore/TesteJoin/orderdetails")

# Salvando DataFrame 'orders' em Parquet
orders.write.parquet("FileStore/TesteJoin/orders")

# Salvando DataFrame 'payments' em Parquet
payments.write.parquet("FileStore/TesteJoin/payments")

# Salvando DataFrame 'product_lines' em Parquet
product_lines.write.parquet("FileStore/TesteJoin/product_lines")

# Salvando DataFrame 'products' em Parquet
products.write.parquet("FileStore/TesteJoin/products")


# COMMAND ----------

# Lendo arquivos Parquet como DataFrames.
# Neste trecho de código, estamos lendo os arquivos Parquet que foram previamente
# salvos como DataFrames. Cada arquivo Parquet é lido usando o método read.parquet,
# resultando em um DataFrame que contém os dados do arquivo.

# Lendo arquivo Parquet 'customers'
customers_parquet = spark.read.parquet("/FileStore/TesteJoin/customers")

# Lendo arquivo Parquet 'employees'
employees_parquet = spark.read.parquet("/FileStore/TesteJoin/employees")

# Lendo arquivo Parquet 'offices'
offices_parquet = spark.read.parquet("/FileStore/TesteJoin/offices")

# Lendo arquivo Parquet 'orderdetails'
orderdetails_parquet = spark.read.parquet("/FileStore/TesteJoin/orderdetails")

# Lendo arquivo Parquet 'orders'
orders_parquet = spark.read.parquet("/FileStore/TesteJoin/orders")

# Lendo arquivo Parquet 'payments'
payments_parquet = spark.read.parquet("/FileStore/TesteJoin/payments")

# Lendo arquivo Parquet 'product_lines'
product_lines_parquet = spark.read.parquet("/FileStore/TesteJoin/product_lines")

# Lendo arquivo Parquet 'products'
products_parquet = spark.read.parquet("/FileStore/TesteJoin/products")


# COMMAND ----------

product_lines_parquet.display()

# COMMAND ----------

# identificar as mudanças entre as fontes de dados
changes = product_lines.subtract(product_lines_parquet)
changes.show()

# COMMAND ----------

def merge_data_sources(df_jdbc, df_parquet, key, path_to_parquet):
    """
    Realiza a ação de merge entre duas fontes de dados: uma DataFrame da fonte JDBC e outra DataFrame
    dos arquivos Parquet. A função identifica registros a serem inseridos, atualizados e excluídos para
    manter a consistência entre as fontes de dados. Os resultados são salvos como arquivos Parquet.

    Args:
        df_jdbc (DataFrame): DataFrame da fonte JDBC contendo os dados atualizados.
        df_parquet (DataFrame): DataFrame dos arquivos Parquet como fonte de armazenamento local.
        key (str): Nome da coluna que é chave primária para identificação de registros.
        path_to_parquet (str): Caminho para onde os arquivos Parquet atualizados serão salvos.

    Returns:
        None: A função não retorna valor. Os resultados são salvos nos arquivos Parquet especificados.
    """
    # Identificar mudanças entre as fontes de dados
    changes = df_jdbc.subtract(df_parquet)

    # Identificar registros a serem excluídos
    deletes = df_parquet.join(changes, key, "inner")

    # Identificar registros a serem inseridos
    inserts = changes

    # Identificar registros a serem atualizados
    updates = df_jdbc.join(df_parquet.alias("parquet_alias"), key, "inner") \
                     .select("parquet_alias.*")

    # Remover registros a serem excluídos dos arquivos Parquet
    df_updated = df_parquet.join(deletes, key, "left_anti")

    # Adicionar registros a serem inseridos aos arquivos Parquet
    df_updated = df_updated.union(inserts)

    # Atualizar registros a serem atualizados nos arquivos Parquet
    df_updated = df_updated.join(updates, updates.columns, "left_anti")

    # Salvar os resultados de volta como Parquet
    df_updated.write.mode("overwrite").parquet(path_to_parquet)

# COMMAND ----------

merge_data_sources(
    df_jdbc=product_lines, 
    df_parquet=product_lines_parquet, 
    key="product_line", 
    path_to_parquet="/FileStore/TesteJoin/product_lines"
)

# COMMAND ----------

new_product_lines_parquet = spark.read.parquet("/FileStore/TesteJoin/product_lines")
new_product_lines_parquet.display()

# COMMAND ----------

# Questão N1: Qual país possui a maior quantidade de itens cancelados?

# Calculando a quantidade de itens cancelados por país:
# - Filtrar os pedidos com status 'Cancelled'
# - Realizar um join com a tabela de clientes baseado no número do cliente
# - Agrupar por país e contar a quantidade de pedidos cancelados por país
# - Ordenar em ordem decrescente pela contagem

cancelled_count_by_country = orders \
    .filter("status = 'Cancelled'") \
    .join(customers, on=orders.customer_number == customers.customer_number, how="inner") \
    .groupBy(customers.country) \
    .count() \
    .orderBy("count", ascending=0)

# Salvando a contagem de itens cancelados por país em formato Delta:
# - Utilizando o método write.mode("overwrite").format("delta").save() para salvar em Delta
# - O caminho "/FileStore/TesteJoin/cancelled_count_by_country.delta" indica onde os dados serão salvos

cancelled_count_by_country.write.mode("overwrite").format("delta").save("/FileStore/TesteJoin/cancelled_count_by_country.delta")

# Exibindo o resultado da contagem de itens cancelados por país
cancelled_count_by_country.display()


# COMMAND ----------

# Questão N2: Qual o faturamento da linha de produto mais vendido?
# Considerando os itens Shipped cujos pedidos foram realizados no ano de 2005.

# Calculando o faturamento da linha de produto mais vendido:
# - Filtrar os pedidos com status 'Shipped' e que foram realizados no ano de 2005
# - Realizar joins para unir os pedidos, detalhes do pedido, produtos e linhas de produto
# - Selecionar a linha de produto e calcular o valor total (quantidade * preço) para cada item
# - Agrupar por linha de produto e somar os valores totais

revenue_by_product_line = orders \
    .filter("status = 'Shipped' AND (order_date >= '2005-01-01' and order_date <= '2005-12-31')") \
    .join(orderdetails, on=orderdetails.order_number == orders.order_number, how="inner") \
    .join(products, on=products.product_code == orderdetails.product_code , how="inner") \
    .join(product_lines, on=product_lines.product_line == products.product_line, how="inner") \
    .select(product_lines.product_line, (orderdetails.quantity_ordered * orderdetails.price_each).alias("amount")) \
    .groupBy(product_lines.product_line) \
    .sum("amount") \
    .alias("revenue")

# Salvando o resultado em formato Delta:
revenue_by_product_line.write.mode("overwrite").format("delta").save("/FileStore/TesteJoin/revenue_by_product_line_delta")

# Exibindo o faturamento da linha de produto mais vendido:
revenue_by_product_line.display()


# COMMAND ----------

# Questão N3: Nome, sobrenome e e-mail dos vendedores do Japão, o local-part do e-mail deve estar mascarado.

import re 
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Definindo a UDF para mascarar o local-part do e-mail
def mask_email(email):
    regex = r'^([^@]+)@'
    match = re.match(regex, email)
    local_part = match.group(1)
    return email.replace(local_part, f"{local_part[0]}{'*' * (len(local_part)-2)}{local_part[-1]}")

# Criando a UDF
mask_email_udf = udf(mask_email, StringType())

# Filtrar os vendedores do escritório com código '5' (Japão)
japan_employees = employees \
    .filter("office_code = '5'") \
    .withColumn("e-mail", mask_email_udf("email")) \
    .select("first_name", "last_name", "e-mail")

# Salvando o resultado em formato Delta:
japan_employees.write.mode("overwrite").format("delta").save("/FileStore/TesteJoin/japan_employees.delta")

# Exibindo os vendedores do escritório '5' com e-mails mascarados
japan_employees.display()


# COMMAND ----------

# MAGIC %fs ls /FileStore/TesteJoin/

# COMMAND ----------

# MAGIC %fs zip customers.zip /FileStore/TesteJoin/customers/
