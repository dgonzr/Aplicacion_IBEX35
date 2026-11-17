from vista.ejecutar_ejercicios import *
import os
import random
import numpy as np

# usamos una semilla desde el principio del programa para reproducibilidad de los modelos de ML
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

practica = Practica()

practica.practica1()
practica.practica2()
practica.practica3()
