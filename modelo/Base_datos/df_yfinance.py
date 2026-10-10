import yfinance as yf
from sesion_spark import spark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

#Ej 1-a
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
#creamos el dataframe con el primer histórico de la empresa ANA.MC
df_total = descargar_histórico("ANA.MC", "2024-10-01", "2026-10-01")
df_total = df_total.withColumn("Ticker", lit("ANA.MC"))  #añadimos la columna Ticker con el valor de la empresa

#mediante un bucle for vamos creando el histórico de todas las demás y uniendo los dataframes en uno solo
empresas = ["ACX.MC", "ACS.MC", "AENA.MC", "AMS.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "COL.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "LOG.MC", "MAP.MC", "MRL.MC", "NTGY.MC", "PUIG.MC", "RED.MC", "REP.MC", "ROVI.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC"]
for empresa in empresas:
    df = descargar_histórico(empresa, "2024-10-01", "2026-10-01")
    df = df.withColumn("Ticker", lit(empresa))  #añadimos la columna Ticker con el valor de la empresa
    df_total = df_total.union(df)

#df_total.write.mode("overwrite").parquet("modelo/data/lake/bronze/ibex")
