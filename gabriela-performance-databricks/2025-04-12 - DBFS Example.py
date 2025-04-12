# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/student_por.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "student_por_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `student_por_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "student_por_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# Leitura dos arquivos CSV
df_por = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/student_por.csv")

# Exibição dos dados
df_por.display()

# COMMAND ----------

# Leitura dos dois arquivos CSV (português)
df_por = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/student_por.csv")

df_total = df_por
df_total.display()


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Criando tabela de dimensão aluno
df_dim_aluno = df_total.select(
    "sex", "age", "famsize", "Pstatus", "Medu", "Fedu", "studytime", 
    "activities", "schoolsup", "famsup", "paid", "higher", 
    "internet", "romantic", "famrel", "freetime", "goout", 
    "Dalc", "Walc", "health"
).dropDuplicates()

# Adicionando uma coluna de ID
df_dim_aluno = df_dim_aluno.withColumn("id_dim_aluno", monotonically_increasing_id())
df_dim_aluno.display()


# COMMAND ----------

# Junção com df_total para trazer o id_dim_aluno
df_fato = df_total.join(df_dim_aluno, on=[
    "sex", "age", "famsize", "Pstatus", "Medu", "Fedu", "studytime", 
    "activities", "schoolsup", "famsup", "paid", "higher", 
    "internet", "romantic", "famrel", "freetime", "goout", 
    "Dalc", "Walc", "health"
], how="left")

df_fato = df_fato.select(
    "id_dim_aluno", "G1", "G2", "G3", "absences"
).withColumnRenamed("G1", "nota_g1") \
 .withColumnRenamed("G2", "nota_g2") \
 .withColumnRenamed("G3", "nota_g3") \
 .withColumnRenamed("absences", "faltas")

df_fato.display()


# COMMAND ----------

# Salvando dimensão aluno
df_dim_aluno.write.format("delta").mode("overwrite").save("/delta/dim_aluno")
spark.sql("DROP TABLE IF EXISTS dim_aluno")
spark.sql("CREATE TABLE dim_aluno USING DELTA LOCATION '/delta/dim_aluno'")

# Salvando tabela fato
df_fato.write.format("delta").mode("overwrite").save("/delta/fato_desempenho")
spark.sql("DROP TABLE IF EXISTS fato_desempenho")
spark.sql("CREATE TABLE fato_desempenho USING DELTA LOCATION '/delta/fato_desempenho'")


# COMMAND ----------

from pyspark.sql.functions import col, isnan, when, count

df_fato.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df_fato.columns]).show()
df_dim_aluno.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df_dim_aluno.columns]).show()


# COMMAND ----------

# MAGIC %md
# MAGIC 1. Pessoas do sexo feminino possuem médias superiores ao masculinho ? - Sim!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sex, ROUND(AVG(nota_g3), 2) AS media_final
# MAGIC FROM fato_desempenho f
# MAGIC JOIN dim_aluno d ON f.id_dim_aluno = d.id_dim_aluno
# MAGIC GROUP BY sex;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Quanto maior o tempo estudando, melhor será a média final? SIm!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT studytime, ROUND(AVG(nota_g3), 2) AS media_final
# MAGIC FROM fato_desempenho f
# MAGIC JOIN dim_aluno d ON f.id_dim_aluno = d.id_dim_aluno
# MAGIC GROUP BY studytime
# MAGIC ORDER BY studytime;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Medu, ROUND(AVG(nota_g3), 2) AS media_mae,
# MAGIC        Fedu, ROUND(AVG(nota_g3), 2) AS media_pai
# MAGIC FROM fato_desempenho f
# MAGIC JOIN dim_aluno d ON f.id_dim_aluno = d.id_dim_aluno
# MAGIC GROUP BY Medu, Fedu
# MAGIC ORDER BY Medu DESC, Fedu DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Alunos que trabalham tem média inferior aos que não trabalham ?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workday, ROUND(AVG(nota_g3), 2) AS media_final
# MAGIC FROM (
# MAGIC     SELECT *,
# MAGIC            CASE WHEN paid = 'yes' THEN 'Sim' ELSE 'Não' END AS workday
# MAGIC     FROM dim_aluno
# MAGIC ) a
# MAGIC JOIN fato_desempenho f ON a.id_dim_aluno = f.id_dim_aluno
# MAGIC GROUP BY workday;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schoolsup, ROUND(AVG(nota_g3), 2) AS media_final
# MAGIC FROM fato_desempenho f
# MAGIC JOIN dim_aluno d ON f.id_dim_aluno = d.id_dim_aluno
# MAGIC GROUP BY schoolsup;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Atividades Extracurriculares ajudam ?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT activities, ROUND(AVG(nota_g3), 2) AS media_final
# MAGIC FROM fato_desempenho f
# MAGIC JOIN dim_aluno d ON f.id_dim_aluno = d.id_dim_aluno
# MAGIC GROUP BY activities;
# MAGIC

# COMMAND ----------

# MAGIC %md 7. Faltas prejudicam o desempenho? - Sim! Quanto mais faltas, pior o desempenho
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE 
# MAGIC            WHEN faltas <= 5 THEN '0-5'
# MAGIC            WHEN faltas <= 10 THEN '6-10'
# MAGIC            WHEN faltas <= 20 THEN '11-20'
# MAGIC            ELSE '21+' 
# MAGIC        END AS grupo_faltas,
# MAGIC        ROUND(AVG(nota_g3), 2) AS media_final
# MAGIC FROM fato_desempenho
# MAGIC GROUP BY grupo_faltas
# MAGIC ORDER BY grupo_faltas;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Consumo de álcool afeta o rendimento? - Sim! Quanto maior o consumo de álcool, menor média final

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Dalc, Walc, ROUND(AVG(nota_g3), 2) AS media_final
# MAGIC FROM fato_desempenho f
# MAGIC JOIN dim_aluno d ON f.id_dim_aluno = d.id_dim_aluno
# MAGIC GROUP BY Dalc, Walc
# MAGIC ORDER BY Dalc, Walc;
# MAGIC