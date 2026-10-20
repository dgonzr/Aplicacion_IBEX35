import yfinance as yf
from modelo.Base_datos.sesion_spark import spark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from delta.tables import DeltaTable

class Funciones:
    def __init__(self):
        pass

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

        return df_ibex

    def reemplazar_columnas(self, df, antiguo:str, nuevo:str):
        """Reemplaza en el dataframe pasado el string antiguo por uno nuevo en las columnas."""
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.replace(antiguo, nuevo))
        return df

    def parquet_write(self, df, ruta:str, modo:str):
        """Escribe el dataframe en formato parquet en la ruta y con el modo especificado."""
        df.write.mode(modo).parquet(ruta)

    def parquet_read(self, ruta:str, columnas:list = None):
        """Lee el parquet de la ruta pasada. Si se le pasa una lista con nombres de columnas, selecciona solo esas."""
        if columnas is None:    
            df = spark.read.parquet(ruta)
        else:
            df = spark.read.parquet(ruta).select(columnas)
        return df

    def printschema(self, df):
        """Imprime el esquema del dataframe pasado"""
        df.printSchema()

    def mostrar(self, df, n=20):
        """Muestra n filas del dataframe pasado."""
        df.show(n)

    def leer_csv(self, csv:str, header:bool, sep:str):
        """Pasa un csv a dataframe con la ruta del csv, un booleano para decidir si lee la primera fila como nombre de las columnas y el separador."""
        return spark.read.option("header", header).option("sep", sep).csv(csv)

    def contar_filas(self, df):
        """Cuenta las filas de un dataframe."""
        return df.count()

    def convertir_delta(self, ruta:str):
        """Convierte a Delta un dataframe en formato parquet usando la ruta pasada."""
        DeltaTable.convertToDelta(spark, ruta)

    def mostrar_historial_delta(self, ruta:str):
        """Muestra el historial pasada la ruta del conjunto de datos Delta."""
        DeltaTable.forPath(spark, ruta).history().show()

    def mostrar_version_delta(self, ruta:str):
        """Devuelve la versión del conjunto de datos Delta pasada la ruta."""
        return DeltaTable.forPath(spark, ruta).history(1).select("version").collect()[0][0]

    def merge(self, datos_nuevos, ruta_destino:str, columnas:list):
        """Hace un merge pasados los datos_originales, la ruta de la segunda tabla y columnas sobre las que hacer el merge."""
        destino = DeltaTable.forPath(spark, ruta_destino)
        clave = ""
        for x in columnas:
            clave = clave + f"d.{x} = n.{x} "
            if x != columnas[-1]: #si no es el último valor, añadimos un AND para seguir añadiendo
                clave = clave + "AND "
            
        (destino.alias("d").merge(datos_nuevos.alias("n"), clave).whenNotMatchedInsertAll().execute())

    def read_delta(self, ruta:str):
        """Lee y devuelve los datos en la ruta específicada con formato Delta."""
        return spark.read.format("delta").load(ruta) 



