from modelo.MachineLearning.funciones_ML import FuncionesML
from modelo.Base_datos.funciones_parquet import Funciones
ML = FuncionesML()
F = Funciones()

class Ejercicios3:
    def Ejercicio1(self):
        df_gold = F.delta_read("modelo/data/lake/gold")