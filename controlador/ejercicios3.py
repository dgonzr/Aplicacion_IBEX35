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
        ML.configurar_mlflow("Ej1_BBVA", "mlruns")

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