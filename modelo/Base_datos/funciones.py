import yfinance as yf
from sesion_spark import spark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from delta.tables import DeltaTable

class Funciones:
    def descargar_histórico(self, empresa:str, fecha_inicio:str, fecha_fin:str):
        """Descarga el histórico de la empresa seleccionada, entre las dos fechas introducidas en formato yyyy-mm-dd"""
        pdf = yf.Ticker(empresa).history(start=fecha_inicio, end=fecha_fin, interval="1d", auto_adjust=False)
        pdf.reset_index(inplace=True) # Convertir el índice 'Date' a columna
        df = spark.createDataFrame(pdf)
        return df

    def crear_df_ibex(self, fecha_inicio:str, fecha_fin:str):
        """Utiliza la función descargar histórico para descargar y unir la información de todas las empresas que nos interesan"""
        #creamos el dataframe con el primer histórico de la empresa ANA.MC
        df_ibex = self.descargar_histórico("ANA.MC", fecha_inicio, fecha_fin)
        df_ibex = df_ibex.withColumn("Ticker", lit("ANA.MC"))
        
        #mediante un bucle for vamos creando el histórico de todas las demás y uniendo los dataframes en uno solo
        empresas = ["ACX.MC", "ACS.MC", "AENA.MC", "AMS.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "COL.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "LOG.MC", "MAP.MC", "MRL.MC", "NTGY.MC", "PUIG.MC", "RED.MC", "REP.MC", "ROVI.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC"]
        for empresa in empresas:
            df = self.descargar_histórico(empresa, fecha_inicio, fecha_fin)
            df = df.withColumn("Ticker", lit(empresa))  #añadimos la columna Ticker con el valor de la empresa
            df_ibex = df_ibex.union(df)

    def reemplazar_columnas(self, df, antiguo:str, nuevo:str):
        """Reemplaza en el dataframe pasado el string antiguo por uno nuevo en las columnas."""
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.replace(antiguo, nuevo))
        return df

    def parquet_write(self, df, ruta:str, modo:str):
        """Escribe el dataframe en formato parquet en la ruta y con el modo especificado."""
        df.write.mode(modo).parquet(ruta)

    def parquet_read(self, ruta:str):
        """Lee el parquet de la ruta pasada."""
        df = spark.read.parquet(ruta)
        return df

    def parquet_read_seleccion(self, ruta:str, columnas):
            """Lee el parquet de la ruta pasada."""
            df = spark.read.parquet(ruta).select(columnas)
            return df

    def printschema(self, df):
        """Imprime el esquema del dataframe pasado"""
        df.printSchema()

    def show(self, df, n=None):
        """Muestra n filas del dataframe pasado."""
        df.show(n)

    def leer_csv(self, csv:str, header:bool, sep:str):
        """Pasa un csv a dataframe con la ruta del csv, un booleano para decidir si lee la primera fila como nombre de las columnas y el separador."""
        df = spark.read.option("header", header).option("sep", sep).csv(csv)
        return df

    def contar_filas(self, df):
        """Cuenta las filas de un dataframe."""
        return df.count()

    def convertir_delta(self, ruta:str):
        """Convierte a Delta un dataframe en formato parquet usando la ruta pasada."""
        DeltaTable.convertToDelta(spark, ruta)

    def mostrar_historial_delta(self, ruta:str):
        DeltaTable.forPath(spark, ruta).history().show()

    def mostrar_version_delta(self, ruta:str):
        v = DeltaTable.forPath(spark, ruta).history(1).select("version").collect()[0][0]
        return v


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
    print("Cuando se ejecuta por segunda vez, el número de filas es el mismo porque ya se han mezclado antes y ya no hay filas nuevas en el segundo dataset que añadir al primero.")




