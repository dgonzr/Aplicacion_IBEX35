import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
import os
import numpy as np

class FuncionesML:
    def configurar_mlflow(self, nombre: str, ruta: str = "mlruns"):
        """Configura la ruta de seguimiento y crea el experimento."""
        # para poder guardar las runs en la carpeta mlruns
        os.environ["MLFLOW_ALLOW_FILE_STORE"] = "true"

        mlflow.set_tracking_uri(ruta)
        mlflow.set_experiment(nombre)

    def crear_pipeline_kmeans(self, feature_cols: list, k: int, seed: int = 42) -> Pipeline:
        """Construye un Pipeline con VectorAssembler y KMeans."""
        assembler = VectorAssembler(inputCols = feature_cols, outputCol = "features")
        kmeans = KMeans(featuresCol = "features", predictionCol = "prediction", k = k, seed = seed)
        return Pipeline(stages = [assembler, kmeans])

    def entrenar_evaluar_kmeans(self, df, feature_cols: list, k: int, seed: int = 42, medida_distancia: str = "squaredEuclidean", nombre_metrica: str = "silhouette", tags_extra: dict = None):
        """Entrena el pipeline, evalúa con Silhouette y registra la run en MLflow. Los tags_extra añaden 
           información sobre el contexto de cada run."""
        pipeline = self.crear_pipeline_kmeans(feature_cols, k, seed)

        nombre_metrica = (
            "silhouette"
            if medida_distancia == "squaredEuclidean"
            else f"silhouette_{medida_distancia}"
        )

        with mlflow.start_run() as run:
            # entrenamiento y predicción
            model = pipeline.fit(df)
            predictions = model.transform(df)

            # evaluación con Silhouette
            evaluator = ClusteringEvaluator(
                featuresCol = "features",
                predictionCol = "prediction",
                metricName = "silhouette",
                distanceMeasure = medida_distancia
            )
            silhouette = evaluator.evaluate(predictions)

            # registro de parámetros, métricas y modelo en MLflow
            mlflow.log_param("algoritmo", "KMeans")
            mlflow.log_param("k", k)
            mlflow.log_param("seed", seed)
            mlflow.log_param("features", ",".join(feature_cols))

            if tags_extra:
                for clave, valor in tags_extra.items():
                    mlflow.log_param(clave, valor)

            mlflow.log_metric(nombre_metrica, silhouette)
            mlflow.spark.log_model(model, "model")

            return {
                "run_id": run.info.run_id,
                "model": model,
                "predictions": predictions,
                "silhouette": silhouette
            }

    def optimizar_kmeans_hyperopt(self, df, feature_cols: list, k_min: int = 2, k_max: int = 6, max_evals: int = 8, seed: int = 42, medida_distancia: str = "cosine", tags_extra: dict = None):
        """Busca automáticamente el mejor k con Hyperopt y registra cada intento en MLflow."""
        # espacio de búsqueda: enteros entre k_min y k_max (2, 3, 4, 5, 6)
        space = {
            "k": hp.quniform("k", k_min, k_max, 1)
        }

        def objective(params):
            # hp.quniform devuelve float, lo convertimos a entero para KMeans
            k = int(params["k"])

            # fija la inicialización aleatoria de los centroides para que el código y algoritmo KMeans sea determinista
            resultado = self.entrenar_evaluar_kmeans(
                df = df,
                feature_cols = feature_cols,
                k = k,
                seed = seed,
                medida_distancia = medida_distancia,
                tags_extra = tags_extra
            )

            # Hyperopt minimiza 'loss'; devolvemos el negativo de silhouette para maximizarla
            return {
                "loss": -resultado["silhouette"],
                "status": STATUS_OK
            }

        trials = Trials()

        # fija el generador de números del algoritmo de búsqueda (TPE) para que la secuencia de valores de k que explora Hyperopt en cada iteración sea reproducible.
        best = fmin(
            fn = objective,
            space = space,
            algo = tpe.suggest,
            max_evals = max_evals,
            trials = trials,
            rstate = np.random.default_rng(seed)
        )

        mejor_k = int(best["k"])
        return {
            "mejor_k": mejor_k,
            "best_raw": best,
            "trials": trials
        }