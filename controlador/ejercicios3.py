from modelo.MachineLearning.funciones_ML import FuncionesML
from modelo.Base_datos.funciones_parquet import Funciones
import os
import random
import numpy as np

ML = FuncionesML()
F = Funciones()

# usamos una semilla desde el principio del programa para reproducibilidad de los modelos de ML
seed = 42
random.seed(seed)
np.random.seed(seed)

class Ejercicios3:
    def Ejercicio1(self):
        df_gold = F.delta_read("modelo/data/lake/gold")
        df_bbva = F.filtrar_dataset(df_gold, "Ticker", "BBVA.MC")
        df_bbva = F.seleccionar_columnas(df_bbva, "Fecha", "Ticker", "Open", "Close")
        df_bbva = F.particion_unica_ordenada(df_bbva, "Fecha") # para que está igual ordenada y particionada que en el ejercicio 4

        # configuramos el experimento con mlflow    
        ML.configurar_mlflow("Ej1_BBVA")

        # entrenar sin Hyperopt utilizando k = 3 (días de subida, bajada y neutros)
        k = 3
        resultado = ML.entrenar_evaluar_kmeans(
            df = df_bbva,
            feature_cols = ["Open", "Close"],
            k = k,
            seed = seed,
            tags_extra = {"ticker": "BBVA.MC", "ejercicio": "Ej1"}
        )

        predictions = resultado["predictions"]
        num_clusters_generados = predictions.select("prediction").distinct().count()

        return predictions, resultado, num_clusters_generados, k

    def Ejercicio2(self):
        # leemos y filtramos el dataframe como en el ejercicio 1
        df_gold = F.delta_read("modelo/data/lake/gold")
        df_bbva = F.filtrar_dataset(df_gold, "Ticker", "BBVA.MC")
        df_bbva = F.seleccionar_columnas(df_bbva, "Fecha", "Ticker", "Open", "Close")

        # configurar experimento en MLflow para agrupar todas las pruebas de Hyperopt
        ML.configurar_mlflow("Ej2_Hyperopt_BBVA")

        # ejecutar búsqueda con Hyperopt (k en [2, 6]), usando Silhouette con distancia coseno
        resultado_opt = ML.optimizar_kmeans_hyperopt(
            df = df_bbva,
            feature_cols = ["Open", "Close"],
            k_min = 2,
            k_max = 6,
            max_evals = 8,
            seed = seed,
            medida_distancia = "cosine",
            tags_extra = {"ticker": "BBVA.MC", "ejercicio": "Ej2"}
        )

        mejor_k = resultado_opt["mejor_k"]
        mejor_loss = min(resultado_opt["trials"].losses())

        return mejor_k, mejor_loss

    def Ejercicio3(self):
        # llama a la función creada para imprimir las runs del experimento
        ML.obtener_e_imprimir_runs(
            nombre_experimento="Ej2_Hyperopt_BBVA",
            nombre_metrica="silhouette_cosine",
            ascendente=False
        )

    def Ejercicio4(self):
        # preparamos las features con su clave (Ticker, Fecha) y las columnas del Ej1
        df_gold = F.delta_read("modelo/data/lake/gold")
        df_bbva = F.filtrar_dataset(df_gold, "Ticker", "BBVA.MC")
        df_bbva = F.seleccionar_columnas(df_bbva, "Fecha", "Ticker", "Open", "Close")

        # guardamos la versión 1 de las features
        ML.guardar_features(df_bbva, 1)

        # recuperamos la versión 1 y reentrenamos con la misma k y seed del Ej1
        df_v1 = ML.recuperar_features(1)

        ML.configurar_mlflow("Ej4_FeatureStore_BBVA")
        resultado_v1 = ML.entrenar_evaluar_kmeans(
            df=df_v1,
            feature_cols=["Open", "Close"],
            k=3,
            seed=42,
        )

        return resultado_v1
        # comprobación de que obtenemos el mismo resultado que en el Ej1
        print(f"\nSilhouette obtenida con features v1: {resultado_v1['silhouette']:.4f}\n")

        # Guardar las features con un número de versión favorece la reproducibilidad ya que , aunque en el futuro
        # se modifiquen las transformaciones o los datos de origen, siempre podamos recuperar el mismo conjunto
        # de entrada. Al combinar esa versión congelada con una semilla fija (seed=42) en KMeans
        # se elimina tanto la variación en los datos como el azar del algoritmo, garantizando idéntico resultado.
        # También hay que tener en cuenta la distribución en diferentes particiones de parquet, ya que si los datos
        # no están igual ordenados, la semilla del kmeans variará en cada partición. Por ello ordeno los datos en el 
        # ejercicio 1 y en la función de recuperar_features para que se vea el mismo resultado en ambos ejercicios.

    def Ejercicio5(self):

        # filtramos otra empresa de la capa gold
        df_gold = F.delta_read("modelo/data/lake/gold")
        df_santander = F.filtrar_dataset(df_gold, "Ticker", "SAN.MC")
        df_santander = F.seleccionar_columnas(df_santander, "Fecha", "Ticker", "Open", "Close")

        # configuramos un experimento distinto para separar los datos de SAN de los de BBVA
        ML.configurar_mlflow("Ej5_Hyperopt_SAN")

        # repetimos la búsqueda con Hyperopt + MLflow sobre el nuevo conjunto de datos
        ML.optimizar_kmeans_hyperopt(
            df=df_santander,
            feature_cols=["Open", "Close"],
            k_min=2,
            k_max=6,
            max_evals=8,
            seed=42,
            medida_distancia="cosine",
            tags_extra={"ticker": "SAN.MC", "ejercicio": "Ej5"}
        )

        # listamos todos los experimentos y mostramos la mejor run de cada uno
        ML.mostrar_mejores_runs_por_experimento()

        # Organizar las ejecuciones en experimentos separados por algoritmo y conjunto de datos
        # evita mezclar métricas obtenidas sobre empresas con rangos de precios
        # diferentes (como BBVA frente a SAN). De este modo, cada experimento actúa como un
        # contenedor aislado donde se optimiza el mejor 'k' para esa empresa concreta, y después
        # se puede comparar de forma limpia qué configuración funciona mejor en cada empresa.
    