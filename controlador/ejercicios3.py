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

        # comprobación pedida por consola
        print(f"Run ID en MLflow: {resultado['run_id']}")
        print(f"Número de agrupaciones configuradas (k): {k}")
        print(f"Número de agrupaciones generadas en predicciones: {num_clusters_generados}")
        print(f"Métrica Silhouette: {resultado['silhouette']:.4f}")
        print("\nDistribución de días por clúster:")
        predictions.groupBy("prediction").count().orderBy("prediction").show()


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

        # comprobación por pantalla
        print(f"Número de agrupaciones (k) seleccionado por Hyperopt: {mejor_k}")
        print(f"Mejor Silhouette (distancia coseno) obtenida: {-mejor_loss:.4f}")
        print("Usando la distancia coseno en la métrica se obtiene un silhouette bajo debido a que los precios de apertura y cierre no tienen mucha" \
        "diferencia entre sí entonces el ángulo es pequeño y el coseno también. Por ello la métrica con distancia euclídea del ejercicio anterior" \
        "es mejor para este caso.\n")
    