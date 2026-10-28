from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, desc, regexp_replace

spark = SparkSession.builder \
    .appName("Analisis_Batch_SECOP_II_Colombia") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n========================================================")
print("1. CARGA DEL DATASET SECOP II (DATOS ABIERTOS COLOMBIA)")
print("========================================================")
df_raw = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv("secop_ii_contratos.csv")

print(f"Total de registros cargados inicialmente: {df_raw.count()}")

columnas_clave = [
    "nombre_entidad", "departamento", "ciudad", "sector",
    "modalidad_de_contratacion", "tipo_de_contrato",
    "estado_contrato", "valor_del_contrato"
]
df_raw.select(*columnas_clave).printSchema()

print("\n========================================================")
print("2. LIMPIEZA Y TRANSFORMACION DE DATOS (DATAFRAMES Y RDD)")
print("========================================================")
df_clean = df_raw.select(*columnas_clave) \
    .na.drop(subset=["departamento", "modalidad_de_contratacion", "valor_del_contrato"]) \
    .withColumn("valor_num", regexp_replace(col("valor_del_contrato").cast("string"), "[^0-9.]", "").cast("double")) \
    .filter((col("valor_num").isNotNull()) & (col("valor_num") > 0)) \
    .filter(col("departamento") != "No Definido")

print(f"Total de registros limpios y validos: {df_clean.count()}")

rdd_modalidad = df_clean.select("modalidad_de_contratacion").rdd \
    .map(lambda row: (row[0], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print("\n--- TOP 5 MODALIDADES DE CONTRATACION (PROCESADO CON RDDs) ---")
for modalidad, total in rdd_modalidad.take(5):
    print(f"Modalidad: {modalidad} | Total contratos: {total}")

print("\n========================================================")
print("3. ANALISIS EXPLORATORIO DE DATOS (EDA) CON DATAFRAMES")
print("========================================================")

print("\n--- TOP 10 DEPARTAMENTOS CON MAYOR EJECUCION PRESUPUESTAL ---")
df_deptos = df_clean.groupBy("departamento") \
    .agg(
        count("*").alias("total_contratos"),
        _sum("valor_num").alias("presupuesto_total_cop"),
        avg("valor_num").alias("valor_promedio_cop")
    ) \
    .orderBy(desc("presupuesto_total_cop"))

df_deptos.show(10, truncate=False)

print("\n--- TOP 10 SECTORES POR CANTIDAD DE PROCESOS CONTRACTUALES ---")
df_sectores = df_clean.groupBy("sector") \
    .agg(
        count("*").alias("total_contratos"),
        _sum("valor_num").alias("presupuesto_total_cop")
    ) \
    .orderBy(desc("total_contratos"))

df_sectores.show(10, truncate=False)

print("\n--- DISTRIBUCION POR ESTADO DEL CONTRATO ---")
df_clean.groupBy("estado_contrato").count().orderBy(desc("count")).show(10, truncate=False)

spark.stop()
