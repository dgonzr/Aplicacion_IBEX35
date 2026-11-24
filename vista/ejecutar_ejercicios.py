from controlador.ejercicios1 import Ejercicios1
from controlador.ejercicios2 import Ejercicios2
from controlador.ejercicios3 import Ejercicios3

class Practica:
    def __init__(self):
        self.entrega1 = Ejercicios1()
        self.entrega2 = Ejercicios2()
        self.entrega3 = Ejercicios3()

    def practica1(self):
        print("Ej 1a\n")
        self.entrega1.Ejercicio1a()
        print("Ej 1b\n")
        self.entrega1.Ejercicio1b()
        print("Ej 1c\n")
        self.entrega1.Ejercicio1c()
        print("Ej 2a\n")
        df = self.entrega1.Ejercicio2a()
        print("Ej 2b\n")
        self.entrega1.Ejercicio2b(df)
        print("Ej 2c\n")
        self.entrega1.Ejercicio2c(df)
        print("Ej 3\n")
        self.entrega1.Ejercicio3()
        print("Ej 4\n")
        self.entrega1.Ejercicio4()
        print("Ej 5a\n")
        self.entrega1.Ejercicio5a()
        print("Ej 5b\n")
        self.entrega1.Ejercicio5b()
        print("Ej 5c\n")
        self.entrega1.Ejercicio5c()
        print("Ej 5d\n")
        self.entrega1.Ejercicio5d()
        print("Ej 6\n")
        self.entrega1.Ejercicio6()
        print("Ej 7\n")
        self.entrega1.Ejercicio7()
        print("Ej 8\n")
        self.entrega1.Ejercicio8()
        print("Ej 9\n")
        self.entrega1.Ejercicio9()
        print("Ej 10\n")
        self.entrega1.Ejercicio10()
        print("Ej 13\n")
        self.entrega1.Ejercicio13()
        pass

    def practica2(self):
        print("Ejercicio 1 práctica 2\n")
        self.entrega2.Ejercicio1()

    def practica3(self):
        print("Ejercicio 1 práctica 3\n")
        self.entrega3.Ejercicio1()
        print("Ejercicio 2 práctica 3\n")
        self.entrega3.Ejercicio2()
        print("Ejercicio 3 práctica 3\n")
        self.entrega3.Ejercicio3()
        print("Ejercicio 4 práctica 3\n")
        self.entrega3.Ejercicio4()
        print("Ejercicio 5 práctica 3\n")
        self.entrega3.Ejercicio5()

