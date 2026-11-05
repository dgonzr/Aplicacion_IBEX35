from modelo.Base_datos.funciones import Funciones
F = Funciones()

class Ejercicios:
    def __init__(self):
        pass

    def Ejercicio1a(self):
        print("Ej 1-a")

        print("\nColumna fecha: no admite nulos, es necesario saber de que día es cada columna\n" \
        "Columnas Open, High, Low y Close: no admiten nulos, todas las empresas tienen que tener un precio de apertura, máximo, mínimo y cierre\n" \
        "Columna Volume: sí admite nulos, puede que alguna empresa no tenga volumen de negociación en algún día\n" \
        "Columna Dividends: no admite nulos, si alguna empresa no tiene dividendo, se indica con 0.0\n" \
        "Columna Stock Splits: no admite nulos, si alguna empresa no tiene splits, se indica con 0.0\n" \
        "Columna Ticker: no admite nulos, es necesario saber a que empresa pertenece cada fila\n")

    def Ejercicio1b(self):
        print("Ej 1-b\n")
        #creamos el dataframe con los históricos de las empresas
        df_ibex = F.crear_df_ibex("2024-10-01", "2026-10-01")

        df_ibex = F.reemplazar_columnas(df_ibex, " ", "_")
        F.parquet_write(df_ibex, "modelo/data/lake/bronze/ibex", "overwrite")

    def Ejercicio1c(self):
        print("Ej 1-c\n")
        df_ibex = F.parquet_read("modelo/data/lake/bronze/ibex")
        F.printschema(df_ibex)
        F.mostrar(df_ibex, 5)

        columnas_seleccion = ["Date", "Close", "Ticker"]
        df_ibex_seleccion = F.parquet_read("modelo/data/lake/bronze/ibex", columnas_seleccion)
        F.mostrar(df_ibex_seleccion)

        print("Leer solo estas tres columnas es más eficiente que leer todas las columnas y luego seleccionar" \
        " solo estas tres, ya que se reduce la cantidad de datos leídos y con estas columnas ya te puedes hacer una idea" \
        " de la evolución de cada empresa a lo largo del tiempo.\n")

    def Ejercicio2a(self):
        print("Ej 2-a\n")
        df_sentimiento = F.leer_csv("modelo/data/sentimiento_ibex.csv", True, ";")
        F.printschema(df_sentimiento)
        print("Tenemos tres columnas: Fecha, Sector y Sentimiento. Las tres vienen en formato de string al leerlo de un csv.\n")
        return df_sentimiento

    def Ejercicio2b(self, df_sentimiento):
        print("Ej 2-b\n")
        print(f"Numero de filas en el dataframe de sentimiento: {F.contar_filas(df_sentimiento)}\n")
        F.parquet_write(df_sentimiento, "modelo/data/lake/bronze/sentimiento", "overwrite")

    def Ejercicio2c(self, df_sentimiento):
        print("Ej 2-c\n")
        F.parquet_write(df_sentimiento, "modelo/data/lake/bronze/sentimiento", "append")
        df_repetido = F.parquet_read("modelo/data/lake/bronze/sentimiento")
        print(f"Numero de filas en el dataframe de sentimiento después de la carga con append: {F.contar_filas(df_repetido)}\n")
        print("El numero de filas de duplica debido a que al hacer el append, añades al final las filas nuevas," \
        "no sobreescribes nada. Entonces almacenas otra vez la misma información y las filas se duplican.\n")
        F.parquet_write(df_sentimiento, "modelo/data/lake/bronze/sentimiento", "overwrite") #vuelvo a hacer un overwrite con el dataframe inicial para que no esté la información repetida"""

    def Ejercicio3(self):
        print("Ej 3\n")
        F.convertir_delta("parquet.`modelo/data/lake/bronze/ibex`")
        F.convertir_delta("parquet.`modelo/data/lake/bronze/sentimiento`")
        F.mostrar_historial_delta("modelo/data/lake/bronze/ibex")
        F.mostrar_historial_delta("modelo/data/lake/bronze/sentimiento")
        print("Aparecen las versiones e información de los datos (como la operación que ocasionó el cambio de versión) guardados," \
        " aunque por ahora solo hay una versión de cada uno.\n")

    def Ejercicio4(self):
        print("Ej 4\n")
        v = F.mostrar_version_delta("modelo/data/lake/bronze/ibex")
        print(f"Versión de la base de datos del ibex: {v}")

        # volvemos a cargar los datos pero tomando como fecha final hoy (20/10/2026)
        df_ibex_hoy = F.crear_df_ibex("2024-10-01", "2026-10-20")

        df_ibex_hoy = F.reemplazar_columnas(df_ibex_hoy, " ", "_")

        df_ibex = F.delta_read("modelo/data/lake/bronze/ibex")
        print(f"Número de filas antes del merge: {F.contar_filas(df_ibex)}")

        F.merge(df_ibex_hoy, "modelo/data/lake/bronze/ibex", ["Ticker", "Date"])
        df_ibex_merge = F.delta_read("modelo/data/lake/bronze/ibex")

        print(f"Número de filas después del merge: {F.contar_filas(df_ibex_merge)}\n")
        print(f"El dataframe pasa de {F.contar_filas(df_ibex)} filas a {F.contar_filas(df_ibex_merge)} filas al hacer el merge por primera vez")
        print("Cuando se ejecuta por segunda vez, el número de filas es el mismo porque ya se han mezclado antes " \
        "y ya no hay filas nuevas en el segundo dataset que añadir al primero.\n")

    def Ejercicio5a(self):
        print("Ej 5a\n")
        df_ibex = F.delta_read("modelo/data/lake/bronze/ibex")
        df_ibex = F.añadir_columna_sumada(df_ibex, "Close", "Close_mas5", 5)

        try:
            F.delta_write(df_ibex, "modelo/data/lake/bronze/ibex", "append", "false", "false")
        except Exception as e:
            print("Efectivamente da error porque los esquemas no coinciden, ya la tabla nueva tiene una columnas de más.\n")

    def Ejercicio5b(self):
        print("Ej 5b\n")
        df_ibex = F.delta_read("modelo/data/lake/bronze/ibex")
        df_ibex = F.añadir_columna_sumada(df_ibex, "Close", "Close_mas5", 5)
        
        F.delta_write(df_ibex, "modelo/data/lake/bronze/ibex", "append", "true", "false") # ahora con mergeSchema True
        print("Ahora sí que lo escribe, ya que con mergeSchema True no hace comprobación de esquemas.\n")

    def Ejercicio5c(self):
        print("Ej 5c\n")
        df_ibex = F.delta_read("modelo/data/lake/bronze/ibex")
        print(f"Número de filas antes de eliminar las filas con Ticker BBVA: {F.contar_filas(df_ibex)}")
        F.eliminar_filas("modelo/data/lake/bronze/ibex", "Ticker = 'BBVA.MC'")
        print(f"Número de filas después de eliminar las filas con Ticker BBVA: {F.contar_filas(df_ibex)}\n")

    def Ejercicio5d(self):
        print("Ej 5d\n")
        F.mostrar_historial_delta("modelo/data/lake/bronze/ibex")
        df_ibex_v1 = F.mostrar_version("modelo/data/lake/bronze/ibex", 1)
        F.mostrar(df_ibex_v1)

    def Ejercicio6(self):
        print("Ej 6\n")
        F.volver_version("modelo/data/lake/bronze/ibex", 1)
        F.mostrar_historial_delta("modelo/data/lake/bronze/ibex")

    def Ejercicio7(self):
        print("Ej 7\n")
        df_ibex = F.delta_read("modelo/data/lake/bronze/ibex")
        df_sentimiento = F.delta_read("modelo/data/lake/bronze/sentimiento")
        F.printschema(df_ibex)
        F.printschema(df_sentimiento)
        print(f"Tamaño de la base de datos del ibex35: {F.contar_filas(df_ibex)} x {F.contar_columnas(df_ibex)}")
        print(f"Tamaño de la base de datos de sentimientos: {F.contar_filas(df_sentimiento)} x {F.contar_columnas(df_sentimiento)}\n")
        F.mostrar(F.contar_valores_unicos(df_sentimiento, "Sector"))
        F.mostrar(F.obtener_valores_unicos(df_sentimiento, "Sector"), 48) # mostramos los 48 valores únicos
        print("La columna de sector de la base de datos de sentimiento tiene muchos valores incorrectamente escritos. En el ejercicio 8 los normalizaremos.\n")

        for column in df_sentimiento.columns:   
            print(f"Hay {F.contar_valores_nulos(df_sentimiento, column)} valores nulos en la columna {column}")
        print("No hay valores nulos en ninguna fila, por lo que la base de datos de sentimientos ya está sin filas duplicadas, valores correctos en" \
        "la columna sector y sin nulos.\n")

        for column in df_ibex.columns:   
            print(f"Hay {F.contar_valores_nulos(df_ibex, column)} valores nulos en la columna {column}")
        print("No hay nulos en la base de datos de yahoo.\n")


        print("Los datos de sentimientos por sector se pueden tener en cuenta a la hora de analizar la base de datos el ibex y ver si existe una " \
        "correlación con las subidas y/o bajadas de cada empresa en la bolsa. Si existe esta correlación, podríamos pronosticar subidas o bajadas en " \
        "bolsa de empresas en un sector específico dependiendo de sucesos que ocurran y provoquen el cambio del sentimiento por ese sector.\n")

    def Ejercicio8(self):
        print("Ej 8\n")
        df_sentimiento = F.delta_read("modelo/data/lake/bronze/sentimiento")
        # Nos interesa ver cuantos valores distintos hay en la columna de sectores
        df_sentimiento = F.normalizar_columna(df_sentimiento, "Sector") # normalizamos y volvemos a mostrar valores únicos
        F.mostrar(F.contar_valores_unicos(df_sentimiento, "Sector"))
        F.mostrar(F.obtener_valores_unicos(df_sentimiento, "Sector"), 48)
        print("Después de normalizar la columna Sector pasamos de 48 a 12 valores únicos. Ahora que hemos normalizado eliminemos posibles columnas duplicadas.")
        
        print(f"Número de filas antes: {F.contar_filas(df_sentimiento)}")
        df_sentimiento = F.eliminar_filas_duplicadas(df_sentimiento)
        print(f"Número de filas después: {F.contar_filas(df_sentimiento)}")
        print("No hay filas duplicadas.\n")


        df_sentimiento = F.cambiar_datatype(df_sentimiento, "Sentimiento", "decimal(10,2)")
        df_sentimiento = F.cambiar_datatype(df_sentimiento, "Fecha", "Timestamp")
        F.printschema(df_sentimiento)
        F.delta_write(df_sentimiento, "modelo/data/lake/silver/sentimiento", "overwrite", "false", "true") # creamos la capa silver para la base de datos de sentimiento

    def Ejercicio9(self):
        print("Ej 9\n")
        df_ibex = F.delta_read("modelo/data/lake/bronze/ibex")
        print(f"Número de filas antes: {F.contar_filas(df_ibex)}")
        df_ibex = F.eliminar_filas_duplicadas(df_ibex)
        print(f"Número de filas después: {F.contar_filas(df_ibex)}")
        print("Este dataframe al sacarlo de Yahoo no tiene duplicados y ya viene en formato limpio.")

        df_ibex = F.añadir_sectores(df_ibex)
        F.printschema(df_ibex)
        F.mostrar(df_ibex, 5)
        F.mostrar(F.contar_valores_unicos(df_ibex, "Ticker"))
        F.mostrar(F.contar_valores_unicos(df_ibex, "Sector"))
        F.mostrar(F.obtener_valores_unicos(df_ibex, "Sector"))

        F.delta_write(df_ibex, "modelo/data/lake/silver/ibex", "overwrite", "true", "false") # creamos la capa silver para la base de datos de sentimiento

    def Ejercicio10(self):
        print("Ej 10\n")
        df_ibex = F.delta_read("modelo/data/lake/silver/ibex")
        df_sentimiento = F.delta_read("modelo/data/lake/silver/sentimiento")
        df_ibex = F.reemplazar_columnas(df_ibex, "Date", "Fecha")
        F.printschema(df_ibex)
        F.printschema(df_sentimiento)
        df = F.join(df_ibex, df_sentimiento, "inner", "Fecha", "Sector")
        print(f"Dimensiones del dataframe combinado: {F.contar_filas(df)} x {F.contar_columnas(df)}")
        for column in df.columns:   
            print(f"Hay {F.contar_valores_nulos(df, column)} valores nulos en la columna {column}")

        print("Al hacer un inner join, las filas que no tienen valores de Fecha y Sector coincidentes no se añaden, por lo que no hay nulos.")

        F.delta_write(df, "modelo/data/lake/gold", "overwrite", "false", "false")

    def Ejercicio13(self):
        df = F.delta_read("modelo/data/lake/gold")

        # Dimensión Ticker
        df_ticker = F.crear_dimension_ticker(df)

        # Dimensión Fecha
        df_fecha = F.crear_dimension_fecha(df)

        # Hechos
        df_hechos = F.crear_hechos(df)

        F.mostrar(df_ticker)
        print(F.contar_filas(df_ticker))
        F.mostrar(df_fecha)
        F.mostrar(df_hechos)

        F.parquet_write(df_ticker, "modelo/data/export/gold/dim_ticker", "overwrite")
        F.parquet_write(df_fecha, "modelo/data/export/gold/dim_fecha", "overwrite")
        F.parquet_write(df_hechos, "modelo/data/export/gold/hechos", "overwrite")

