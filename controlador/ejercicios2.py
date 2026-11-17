from modelo.Base_datos.funciones_parquet import Funciones
F = Funciones()

class Ejercicios2:
    def Ejercicio1(self):
        df_dim_ticker = F.parquet_read("modelo/data/export/gold/dim_ticker")
        df_dim_fecha = F.parquet_read("modelo/data/export/gold/dim_fecha")
        df_hechos = F.parquet_read("modelo/data/export/gold/hechos")

        # se reduce el número de particiones a 1 en cada carpeta de la capa gold para exportar
        F.coalesce(df_dim_ticker, 1, "modelo/data/export/gold/dim_ticker", "overwrite")
        F.coalesce(df_dim_fecha, 1, "modelo/data/export/gold/dim_fecha", "overwrite")
        F.coalesce(df_hechos, 1, "modelo/data/export/gold/hechos", "overwrite")