from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("job-1-spark") \
    .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
    .config("fs.s3a.endpoint", "http://192.xxx.xxx.xxx.:9000") \
    .config("fs.s3a.access.key", "minioadmin")\
    .config("fs.s3a.secret.key", "minioadmin")\
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.path.style.access", "True")\
    .getOrCreate()

# definindo o método de logging da aplicação use INFO somente para DEV [INFO,ERROR]
spark.sparkContext.setLogLevel("INFO")

# lendo os dados do Data Lake
# s3a = protocolo de comunicação e segurança com o MinIO

df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema","True")\
    .csv("s3a://landing/*.csv") # databricks .load('caminho_landing') aqui na api spark .csv('caminho_minio')

# imprime os dados lidos da raw de landing
print ("\nImprime os dados lidos da lading:")
print (df.show())

# imprime o schema do dataframe
print ("\nImprime o schema do dataframe lido da raw de landing:")
print (df.printSchema())

# converte para formato parquet
print ("\nEscrevendo os dados lidos da raw para parquet na processing zone...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("s3a://processing/df-parquet-file.parquet")

# lendo arquivos parquet
df_parquet = spark.read.format("parquet")\
 .load("s3a://processing/df-parquet-file.parquet")

# imprime os dados lidos em parquet de processing
print ("\nImprime os dados lidos em parquet da processing zone")
print (df_parquet.show())

# cria uma view para trabalhar com sql com o dataframe em formato parquet
df_parquet.createOrReplaceTempView("view_df_parquet")

# processa os dados conforme regra de negócio
df_result = spark.sql("SELECT BNF_CODE as Bnf_code \
                       ,SUM(ACT_COST) as Soma_Act_cost \
                       ,SUM(QUANTITY) as Soma_Quantity \
                       ,SUM(ITEMS) as Soma_items \
                       ,AVG(ACT_COST) as Media_Act_cost \
                      FROM view_df_parquet \
                      GROUP BY bnf_code \
                      ORDER BY Bnf_code")

# imprime o resultado do dataframe criado
print ("\n ========= Imprime o resultado do dataframe processado =========\n")
print (df_result.show())

# converte para formato parquet
print ("\nEscrevendo os dados processados na Curated Zone...")

# converte os dados processados para parquet e escreve na curated zone
df_result.write.format("parquet")\
         .mode("overwrite")\
         .save("s3a://curated/df-result-file.parquet")

df_final_resut = spark.read.format('parquet')\
                 .load('s3a://curated/df-result-file.parquet')

print ("\n ========= Imprime resultado final =========\n")
print(df_final_resut.show())     


# para a aplicação
spark.stop()
