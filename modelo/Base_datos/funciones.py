import yfinance as yf
from modelo.Base_datos.sesion_spark import spark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from delta.tables import DeltaTable
from pathlib import Path
import shutil
from uuid import uuid4

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
        #creamos el dataframe con el primer histórico de la empresa ACX.MC
        df_ibex = self.descargar_histórico("ACX.MC", fecha_inicio, fecha_fin)
        df_ibex = df_ibex.withColumn("Ticker", lit("ACX.MC"))
        
        #mediante un bucle for vamos creando el histórico de todas las demás y uniendo los dataframes en uno solo
        empresas = ["ACS.MC", "AENA.MC", "ANA.MC", "ANE.MC", "AMS.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "COL.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "LOG.MC", "MAP.MC", "MRL.MC", "NTGY.MC", "PUIG.MC", "RED.MC", "REP.MC", "ROVI.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC"]
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

    def contar_columnas(self, df):
        return len(df.columns)

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

    def join(self, df1, df2, how:str, *columnas:str):
        """Devuelve el dataframe después de hacer un join de los dos dataframes pasados."""
        columnas = list(columnas)
        nuevo_df = df1.join(df2, on=columnas, how=how)
        return nuevo_df

    def delta_read(self, ruta:str):
        """Lee y devuelve los datos en la ruta específicada con formato Delta."""
        return spark.read.format("delta").load(ruta) 

    def delta_write(self, df, ruta:str, modo:str, mergeSchema:str, overwriteSchema:str):
        """Escribe el dataframe Delta pasado con el modo especificado en la ruta. Si mergeSchema es true, Delta no comprobará el esquema
           de la nueva tabla y lo escribirá. Si overwriteSchema es true, cambia el esquema (tipos de datos)."""
        df.write.format("delta").mode(modo).option("mergeSchema", mergeSchema).option("overwriteSchema", overwriteSchema).save(ruta)

    def añadir_columna_sumada(self, df, columna_referencia: str, nueva_columna: str, valor_sumado: int):
        """Añade una nueva columna que es la columna referencia más un valor pasado."""
        df = df.withColumn(nueva_columna, col(columna_referencia) + valor_sumado)
        return df

    def eliminar_filas(self, ruta:str, condicion:str):
        """Elimina las filas que cumplen la condición."""
        destino = DeltaTable.forPath(spark, "modelo/data/lake/bronze/ibex")
        destino.delete(condicion)

    def eliminar_filas_duplicadas(self, df):
        """Devuelve el dataframe con filas duplicadas eliminadas."""
        return df.distinct()

    def contar_valores_nulos(self, df, columna:str):
        """Devuelve un dataframe con el número de valores nulos en una columna."""
        return df.filter(col(columna).isNull()).count()

    def obtener_valores_nulos(self, df, columna: str):
        """Devuelve un dataframe con las filas donde la columna es nula."""
        return df.filter(col(columna).isNull())

    def contar_valores_unicos(self, df, columna: str):
        """Devuelve un dataframe con el número de valores únicos en una columna."""
        return df.select(count_distinct(columna))

    def obtener_valores_unicos(self, df, columna: str):
        """Devuelve un dataframe con los valores únicos en una columna."""
        return df.select(columna).dropDuplicates()

    def mostrar_version(self, ruta:str, version:int):
        """Devuelve una version anterior de la tabla en la ruta pasada. No cambia la versión actual de los datos."""
        df_v = spark.read.format("delta").option("versionAsOf",version).load(ruta)
        return df_v

    def volver_version(self, ruta:str, version:int):
        """Utiliza restore para cambiar la tabla en la ruta por una versión anterior de ella misma."""
        tabla = DeltaTable.forPath(spark, ruta)
        tabla.restoreToVersion(version)

    def normalizar_columna(self, df, columna: str):
        """Pasado un dataframe y una columna, elimina espacios, quita acentos, pasa todo a minúsculas y lo devuelve."""
        df_clean = df.withColumn(
            columna,
            translate(
            lower(trim(regexp_replace(col(columna), r"\s+", " "))),
            "áéíóúàèìòùñ",
            "aeiouaeioun"
            )
        )

        return df_clean

    def cambiar_datatype(self, df, columna:str, tipo_nuevo:str):
        """Devuelve el dataframe con la columna cambiada al tipo nuevo."""
        return df.withColumn(columna, col(columna).cast(tipo_nuevo))
    
    def añadir_sectores(self, df):
        """Devuelve el dataframe del ibex con los sectores."""
        empresas = ["ACX.MC", "ACS.MC", "AENA.MC", "ANA.MC", "ANE.MC", "AMS.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "COL.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "LOG.MC", "MAP.MC", "MRL.MC", "NTGY.MC", "PUIG.MC", "RED.MC", "REP.MC", "ROVI.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC"]
        dic = {}
        for e in empresas:
            info = yf.Ticker(e).info
            dic[e] = info.get("sector") # lo paso a minusculas porque así está en la base de datos de sentimiento

        get_sector_udf = udf(lambda ticker: dic.get(ticker, "Sin Clasificar"), StringType())
        df_resultado = df.withColumn("Sector", get_sector_udf(col("Ticker")))
        df_resultado = self.normalizar_columna(df_resultado, "Sector")
        return df_resultado

    def crear_dimension_ticker(self, df):
        """Devuelve un dataframe con las columnas Ticker y Sector para la dimensión del Ticker."""
        return df.select(col("Ticker"), col("Sector")).distinct()

    def crear_dimension_fecha(self, df):
            """Devuelve un dataframe no solo con la columna de fecha, sino con el año, mes, trimestre y dia de la semana para la dimensión de la fecha."""
            df_fecha = df.select("Fecha").distinct() \
            .withColumn("Año", year("Fecha")) \
            .withColumn("Mes", month("Fecha")) \
            .withColumn("Trimestre", quarter("Fecha")) \
            .withColumn("Dia_semana", date_format("Fecha", "E"))

            return df_fecha

    def crear_hechos(self, df):
        """Devuelve un dataframe con la tabla de hechos (precios de apertura, cierre, máximos, mínimos...)"""
        return df.select(col("Ticker"), col("Fecha"), col("Open"), col("High"), col("Low"), col("Close"), col("Adj_close"), col("Volume"), col("Dividends"), col("Stock_Splits"), col("Sentimiento"))

    def coalesce(self, df, n: int, ruta: str, modo: str):
        """Reduce el número de particiones de una carpeta Parquet a n."""
        destino = Path(ruta).resolve()
        sufijo = uuid4().hex
        temporal = destino.with_name(f"{destino.name}_tmp_{sufijo}")
        respaldo = destino.with_name(f"{destino.name}_bak_{sufijo}")
        respaldo_creado = False

        # hay que crear carpetas temporales para escribirlo ya que al leer y sobreescribir la misma carpeta da error
        try:
            # Escribe completamente en otra ruta antes de reemplazar el origen.
            df.coalesce(n).write.mode(modo).parquet(str(temporal))

            if destino.exists():
                destino.rename(respaldo)
                respaldo_creado = True

            try:
                temporal.rename(destino)
            except Exception:
                if respaldo_creado and respaldo.exists() and not destino.exists():
                    respaldo.rename(destino)
                    respaldo_creado = False
                raise

            if respaldo_creado:
                shutil.rmtree(respaldo)
        except Exception:
            # Restaura la carpeta original si el reemplazo falla.
            if respaldo_creado and not destino.exists() and respaldo.exists():
                respaldo.rename(destino)
            raise
        finally:
            if temporal.exists():
                shutil.rmtree(temporal)