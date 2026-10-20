from modelo.Base_datos.funciones import Funciones
F = Funciones()

class Ejercicios:
    def __init__(self):
        self.df_ibex = None
        self.df_sentimiento = None

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
        #creamos el dataframe con el primer histórico de la empresa ANA.MC
        df_ibex = F.crear_df_ibex("2024-10-01", "2026-10-01")

        df_ibex = F.reemplazar_columnas(df_ibex, " ", "_")
        F.parquet_write(df_ibex, "modelo/data/lake/bronze/ibex", "overwrite")

    def Ejercicio1c(self):
        print("Ej 1-c\n")
        df_ibex = F.parquet_read("modelo/data/lake/bronze/ibex")
        self.df_ibex = df_ibex # lo guardamos en el atributo de la clase para poder acceder a él desde cualquier ejercicio sin tener que volver a leer el df
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
        self.df_sentimiento = df_sentimiento # lo guardamos en el atributo de la clase para poder acceder a él desde cualquier ejercicio sin tener que volver a leer el df
        F.printschema(df_sentimiento)
        print("Tenemos tres columnas: Fecha, Sector y Sentimiento. Las tres vienen en formato de string al leerlo de un csv.\n")

    def Ejercicio2b(self):
        print("Ej 2-b\n")
        df_sentimiento = self.df_sentimiento
        print(f"Numero de filas en el dataframe de sentimiento: {F.contar_filas(df_sentimiento)}\n")
        F.parquet_write(df_sentimiento, "modelo/data/lake/bronze/sentimiento", "overwrite")

    def Ejercicio2c(self):
        print("Ej 2-c\n")
        df_sentimiento = self.df_sentimiento
        F.parquet_write(df_sentimiento, "modelo/data/lake/bronze/sentimiento", "append")
        df_repetido = F.parquet_read("modelo/data/lake/bronze/sentimiento")
        print(f"Numero de filas en el dataframe de sentimiento después de la carga con append: {F.contar_filas(df_repetido)}\n")
        print("El numero de filas de duplica debido a que al hacer el append, añades al final las filas nuevas," \
        "no sobreescribes nada. Entonces almacenas otra vez la misma información y las filas se duplican.")
        F.parquet_write(df_sentimiento, "modelo/data/lake/bronze/sentimiento", "overwrite") #vuelvo a hacer un overwrite con el dataframe inicial para que no esté la información repetida"""

    def Ejercicio3(self):
        print("Ej 3\n")
        F.convertir_delta("parquet.`modelo/data/lake/bronze/ibex`")
        F.convertir_delta("parquet.`modelo/data/lake/bronze/sentimiento`")
        F.mostrar_historial_delta("modelo/data/lake/bronze/ibex")
        F.mostrar_historial_delta("modelo/data/lake/bronze/sentimiento")
        print("Aparecen las versiones e información de los datos (como los bytes que ocupan) guardados, aunque por ahora solo hay una versión de cada uno.")

    def Ejercicio4(self):
        print("Ej 4\n")
        v = F.mostrar_version_delta("modelo/data/lake/bronze/ibex")
        print(f"Versión de la base de datos del ibex: {v}")

        # volvemos a cargar los datos pero tomando como fecha final hoy (20/10/2026)
        df_ibex_hoy = F.crear_df_ibex("2024-10-01", "2026-10-20")

        df_ibex_hoy = F.reemplazar_columnas(df_ibex_hoy, " ", "_")

        df_ibex = F.read_delta("modelo/data/lake/bronze/ibex")
        print(f"Número de filas antes del merge: {F.contar_filas(df_ibex)}")

        F.merge(df_ibex_hoy, "modelo/data/lake/bronze/ibex", ["Ticker", "Date"])
        df_ibex = F.read_delta("modelo/data/lake/bronze/ibex")
        self.df_ibex = df_ibex

        print(f"Número de filas después del merge: {F.contar_filas(df_ibex)}\n")
        print(f"El dataframe pasa de 17340 filas a 17782 filas al hacer el merge por primera vez")
        print("Cuando se ejecuta por segunda vez, el número de filas es el mismo porque ya se han mezclado antes y ya no hay filas nuevas en el segundo dataset que añadir al primero.")