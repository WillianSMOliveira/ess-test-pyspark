from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, expr, rand, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":

    # Inicializando o Spark
    spark = SparkSession\
        .builder\
        .appName("ess_example")\
        .getOrCreate()
    
    # Lendo arquivo e transformando em um dataframe
    df_csv = spark.read.option("header",True) \
        .csv("Lista_Municípios_com_IBGE_Brasil_Versao_CSV.csv")

    # Tratamentos de exclusão, rename e inclusão de colunas para gerar os dados de acordo com o enunciado
    df_csv.drop("ConcatUF+Mun","IBGE","IBGE7","Região","População 2010","Porte","Capital") \
        .withColumnRenamed("UF","estado") \
        .withColumnRenamed("Município","municipio")

    # Incluindo coluna de data de atualização
    df_with_column = df_csv.df.select("*") \
        .withColumn("data_atualizacao", f.current_date())

    # Incluindo coluna de controle com quantidade de municípios por estado e obtendo 10 municípios por estado
    window_estado = Window.partitionBy("estado")
    df_top_10 = df_with_column.withColumn("row",row_number().over(window_estado)) \
            .filter(col("row") <= 10) \
            .drop("row")
    
    # Duplicando todos os registros 3 (criando um array de dataframes e union depois) vezes para poder haver ao menos 3 transações por município
    # incluindo coluna com números aleatórios para representar uma transação
    dfs = [df_top_10,df_top_10,df_top_10]
    df_with_transacao = reduce(DataFrame.unionAll, dfs) \
        .withColumn("transacao", rand())

    # Ordenando por estado, município e transação
    df = df_with_transacao.orderBy(["estado","municipio","transacao"], ascending=True)
    
    # Exibindo os registros do dataframe
    df.show()
