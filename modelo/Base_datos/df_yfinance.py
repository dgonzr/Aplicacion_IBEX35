import yfinance as yf
from sesion_spark import spark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from delta.tables import DeltaTable

#Ej 1-a
print("Ej 1-a\n")
estructura = StructType([
    StructField("Date", TimestampType(), False),
    StructField("Open", DoubleType(), False),
    StructField("High", DoubleType(), False),
    StructField("Low", DoubleType(), False),
    StructField("Close", DoubleType(), False),
    StructField("Adj Close", DoubleType(), False),
    StructField("Volume", LongType(), True),
    StructField("Dividends", DoubleType(), False),
    StructField("Stock Splits", DoubleType(), False),
])

def descargar_histórico(empresa:str, fecha_inicio:str, fecha_fin:str):
    pdf = yf.Ticker(empresa).history(start=fecha_inicio, end=fecha_fin, interval="1d", auto_adjust=False)
    pdf.reset_index(inplace=True) # Convertir el índice 'Date' a columna
    df = spark.createDataFrame(pdf, schema=estructura)
    return df

print("\nColumna fecha: no admite nulos, es necesario saber de que día es cada columna\n" \
"Columnas Open, High, Low y Close: no admiten nulos, todas las empresas tienen que tener un precio de apertura, máximo, mínimo y cierre\n" \
"Columna Volume: sí admite nulos, puede que alguna empresa no tenga volumen de negociación en algún día\n" \
"Columna Dividends: no admite nulos, si alguna empresa no tiene dividendo, se indica con 0.0\n" \
"Columna Stock Splits: no admite nulos, si alguna empresa no tiene splits, se indica con 0.0\n" \
"Columna Ticker: no admite nulos, es necesario saber a que empresa pertenece cada fila\n")

#Ej 1-b
print("Ej 1-b\n")
#creamos el dataframe con el primer histórico de la empresa ANA.MC
df_ibex = descargar_histórico("ANA.MC", "2024-10-01", "2026-10-01")
df_ibex = df_ibex.withColumn("Ticker", lit("ANA.MC"))  #añadimos la columna Ticker con el valor de la empresa

#mediante un bucle for vamos creando el histórico de todas las demás y uniendo los dataframes en uno solo
empresas = ["ACX.MC", "ACS.MC", "AENA.MC", "AMS.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "COL.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "LOG.MC", "MAP.MC", "MRL.MC", "NTGY.MC", "PUIG.MC", "RED.MC", "REP.MC", "ROVI.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC"]
for empresa in empresas:
    df = descargar_histórico(empresa, "2024-10-01", "2026-10-01")
    df = df.withColumn("Ticker", lit(empresa))  #añadimos la columna Ticker con el valor de la empresa
    df_ibex = df_ibex.union(df)

for col_name in df_ibex.columns:
    df_ibex = df_ibex.withColumnRenamed(col_name, col_name.replace(" ", "_"))

#df_ibex.write.mode("overwrite").parquet("modelo/data/lake/bronze/ibex")

#Ej 1-c
print("Ej 1-c\n")
df_ibex = spark.read.parquet("modelo/data/lake/bronze/ibex")
df_ibex.printSchema()
#df_ibex.show(5)

df_ibex_seleccion = spark.read.parquet("modelo/data/lake/bronze/ibex").select("Date", "Close", "Ticker")
#df_ibex_seleccion.show()

print("Leer solo estas tres columnas es más eficiente que leer todas las columnas y luego seleccionar" \
" solo estas tres, ya que se reduce la cantidad de datos leídos y con estas columnas ya te puedes hacer una idea" \
" de la evolución de cada empresa a lo largo del tiempo.\n")

#Ej 2-a
print("Ej 2-a\n")
df = spark.read.option("header", True).option("sep", ";").csv("modelo/data/sentimiento_ibex.csv")
df.printSchema()
print("Tenemos tres columnas: Fecha, Sector y Sentimiento. Las tres vienen en formato de string al leerlo de un csv.\n")

#Ej 2-b
print("Ej 2-b\n")
print(f"Numero de filas en el dataframe de sentimiento: {df.count()}\n")
#df.write.mode("overwrite").parquet("modelo/data/lake/bronze/sentimiento")

#Ej 2-c
print("Ej 2-c\n")
"""df.write.mode("append").parquet("modelo/data/lake/bronze/sentimiento")
df_repetido = spark.read.parquet("modelo/data/lake/bronze/sentimiento")
print(f"Numero de filas en el dataframe de sentimiento después de la carga con append: {df_repetido.count()}\n")
print("El numero de filas de duplica debido a que al hacer el append, añades al final las filas nuevas," \
"no sobreescribes nada. Entonces almacenas otra vez la misma información y las filas se duplican.")
df.write.mode("overwrite").parquet("modelo/data/lake/bronze/sentimiento") #vuelvo a hacer un overwrite para que no esté la información repetida"""

#Ej 3
print("Ej 3\n")
#DeltaTable.convertToDelta(spark, "parquet.`modelo/data/lake/bronze/ibex`")
#DeltaTable.convertToDelta(spark, "parquet.`modelo/data/lake/bronze/sentimiento`")
DeltaTable.forPath(spark, "modelo/data/lake/bronze/ibex").history().show()
DeltaTable.forPath(spark, "modelo/data/lake/bronze/sentimiento").history().show()
print("Aparecen las versiones e información de los datos (como los bytes que ocupan) guardados, aunque por ahora solo hay una versión de cada uno.")

#Ej 4
print("Ej 4\n")
v = DeltaTable.forPath(spark, "modelo/data/lake/bronze/ibex").history(1).select("version").collect()[0][0]
print(f"Versión de la base de datos del ibex: {v}")

#reutilizo el ejercicio 1-b para volver a cargar los datos pero tomando como fecha final hoy (13/10/2026)
df_ibex_hoy = descargar_histórico("ANA.MC", "2024-10-01", "2026-10-13")
df_ibex_hoy = df_ibex_hoy.withColumn("Ticker", lit("ANA.MC"))  #añadimos la columna Ticker con el valor de la empresa

empresas = ["ACX.MC", "ACS.MC", "AENA.MC", "AMS.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "COL.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "LOG.MC", "MAP.MC", "MRL.MC", "NTGY.MC", "PUIG.MC", "RED.MC", "REP.MC", "ROVI.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC"]
for empresa in empresas:
    df = descargar_histórico(empresa, "2024-10-01", "2026-10-13")
    df = df.withColumn("Ticker", lit(empresa))  #añadimos la columna Ticker con el valor de la empresa
    df_ibex_hoy = df_ibex_hoy.union(df)

for col_name in df_ibex_hoy.columns:
    df_ibex_hoy = df_ibex_hoy.withColumnRenamed(col_name, col_name.replace(" ", "_"))

print(f"Número de filas antes del merge: {df_ibex.count()}")
destino = DeltaTable.forPath(spark, "modelo/data/lake/bronze/ibex")
(destino.alias("d").merge(df_ibex_hoy.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date").whenNotMatchedInsertAll().execute())
df_ibex = spark.read.format("delta").load("modelo/data/lake/bronze/ibex") 
print(f"Número de filas después del merge: {df_ibex.count()}")
print(f"El dataframe pasa de 17340 filas a 17612 filas al hacer el merge por primera vez")