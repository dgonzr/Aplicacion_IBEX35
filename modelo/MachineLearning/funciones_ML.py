import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import os

class FuncionesML:
    def configurar_mlflow(self, nombre: str, ruta: str):
        """Configura la ruta de seguimiento y crea el experimento."""
        # para poder guardar las runs en la carpeta mlruns
        os.environ["MLFLOW_ALLOW_FILE_STORE"] = "true"

        mlflow.set_tracking_uri(ruta)
        mlflow.set_experiment(nombre)

    def crear_pipeline_kmeans(self, col_caracteristicas: list, k: int, seed: int = 42) -> Pipeline:
        """Construye un Pipeline con VectorAssembler y KMeans."""
        assembler = VectorAssembler(inputCols = col_caracteristicas, outputCol = "features")
        kmeans = KMeans(featuresCol = "features", predictionCol = "prediction", k = k, seed = seed)
        return Pipeline(stages = [assembler, kmeans])

    def entrenar_evaluar_kmeans(self, df, feature_cols: list, k: int, seed: int = 42, tags_extra: dict = None):
        """Entrena el pipeline, evalúa con Silhouette y registra la run en MLflow. Los tags_extra añaden 
           información sobre el contexto de cada run."""
        pipeline = self.crear_pipeline_kmeans(feature_cols, k, seed)

        with mlflow.start_run() as run:
            # entrenamiento y predicción
            model = pipeline.fit(df)
            predictions = model.transform(df)

            # evaluación con Silhouette
            evaluator = ClusteringEvaluator(
                featuresCol = "features",
                predictionCol = "prediction",
                metricName = "silhouette"
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

            mlflow.log_metric("silhouette", silhouette)
            mlflow.spark.log_model(model, "model")

            return {
                "run_id": run.info.run_id,
                "model": model,
                "predictions": predictions,
                "silhouette": silhouette
            }