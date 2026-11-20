import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
import os
import numpy as np
import pandas as pd
from modelo.Base_datos.funciones_parquet import Funciones

F = Funciones()

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

    def obtener_e_imprimir_runs(self, nombre_experimento: str, nombre_metrica: str = None, ascendente: bool = False):
        """Recupera todas las runs de un experimento en MLflow, las ordena por la métrica
           indicada e imprime el run_id, los parámetros almacenados y el valor de la métrica."""
        
        # garantiza que MLflow apunta a la ruta local ./mlruns aunque se ejecute de forma aislada
        self.configurar_mlflow(nombre_experimento)

        experiment = mlflow.get_experiment_by_name(nombre_experimento)
        if experiment is None:
            print(f"No se encontró el experimento '{nombre_experimento}'.")
            return None

        # recupera las runs del experimento como DataFrame de pandas
        runs_df = mlflow.search_runs(experiment_ids=[experiment.experiment_id])

        if runs_df.empty:
            print(f"El experimento '{nombre_experimento}' no tiene runs registradas.")
            return runs_df

        # identifica la columna de métrica por la que se va a ordenar
        cols_metricas = [c for c in runs_df.columns if c.startswith("metrics.")]
        if nombre_metrica:
            col_metrica = f"metrics.{nombre_metrica}"
        elif cols_metricas:
            col_metrica = cols_metricas[0]
        else:
            print("No se encontraron métricas registradas en las runs.")
            return runs_df

        # ordena las ejecuciones según el valor de la métrica (de mayor a menor por defecto)
        runs_df_sorted = runs_df.sort_values(
            by=col_metrica,
            ascending=ascendente,
            na_position="last"
        )

        # extrae dinámicamente todas las columnas de parámetros guardados
        cols_params = [c for c in runs_df_sorted.columns if c.startswith("params.")]
        etiqueta_metrica = col_metrica.replace("metrics.", "")

        print(f"\nRuns del experimento '{nombre_experimento}' (ordenadas por {etiqueta_metrica}):")
        print("-" * 85)

        for _, row in runs_df_sorted.iterrows():
            run_id = row["run_id"]
            valor_metrica = row[col_metrica]

            # Recopila todos los parámetros almacenados en esa run
            params_run = {
                col.replace("params.", ""): row[col]
                for col in cols_params
                if pd.notna(row[col])
            }

            print(f"Run ID     : {run_id}")
            print(f"Parámetros : {params_run}")
            print(f"{etiqueta_metrica:<11}: {valor_metrica:.4f}")
            print("-" * 85)

        return runs_df_sorted

    def guardar_features(self, df, version: int, ruta: str = "modelo/data/lake/features"):
        """Guarda las features en Parquet añadiendo la columna version."""
        df_version = F.añadir_columna(df, "version", F.convertir_a_columna(version))
        F.parquet_write(df_version, ruta, "append")

    def recuperar_features(self, version: int, ruta: str = "modelo/data/lake/features"):
        """Lee la tabla de features y filtra por la versión indicada."""
        df_features = F.parquet_read(ruta)
        df_features = F.filtrar_dataset(df_features, "version", version)
        df_features = F.eliminar_filas_duplicadas(df_features) # eliminamos filas duplicadas por si al guardar los datos con append, por hacer alguna prueba de más hayan quedado filas repetidas
        return F.particion_unica_ordenada(df_features, "Fecha")
    
    def mostrar_mejores_runs_por_experimento(self):
        """Lista todos los experimentos disponibles y muestra la mejor run de cada uno."""
        mlflow.set_tracking_uri("mlruns")

       # buscamos los experimentos
        experimentos = mlflow.search_experiments()

        print("\nExperimentos disponibles en MLflow:")
        for exp in experimentos:
            print(f" - ID: {exp.experiment_id} | Nombre: {exp.name}")

        print("\nMejor run por experimento:")
        print("-" * 80)
        for exp in experimentos:
            runs_df = mlflow.search_runs(experiment_ids=[exp.experiment_id])
            if runs_df.empty:
                continue

            # detecta automáticamente la métrica usada en ese experimento (silhouette o silhouette_cosine)
            cols_metricas = [c for c in runs_df.columns if c.startswith("metrics.")]
            if not cols_metricas:
                continue

            col_metrica = cols_metricas[0]
            mejor_run = runs_df.sort_values(by=col_metrica, ascending=False).iloc[0]

            cols_params = [c for c in runs_df.columns if c.startswith("params.")]
            params_run = {
                c.replace("params.", ""): mejor_run[c]
                for c in cols_params
                if pd.notna(mejor_run[c])
            }
            etiqueta_metrica = col_metrica.replace("metrics.", "")

            print(f"Experimento : {exp.name}")
            print(f"Mejor Run ID: {mejor_run['run_id']}")
            print(f"Parámetros  : {params_run}")
            print(f"{etiqueta_metrica:<12}: {mejor_run[col_metrica]:.4f}")
            print("-" * 80)