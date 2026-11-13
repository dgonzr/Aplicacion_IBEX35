from controlador.ejercicios import Ejercicios

class Practica:
    def __init__(self):
        self.entrega = Ejercicios()

    def practica1(self):
        print("Ej 1a\n")
        self.entrega.Ejercicio1a()
        print("Ej 1b\n")
        self.entrega.Ejercicio1b()
        print("Ej 1c\n")
        self.entrega.Ejercicio1c()
        print("Ej 2a\n")
        df = self.entrega.Ejercicio2a()
        print("Ej 2b\n")
        self.entrega.Ejercicio2b(df)
        print("Ej 2c\n")
        self.entrega.Ejercicio2c(df)
        print("Ej 3\n")
        self.entrega.Ejercicio3()
        print("Ej 4\n")
        self.entrega.Ejercicio4()
        print("Ej 5a\n")
        self.entrega.Ejercicio5a()
        print("Ej 5b\n")
        self.entrega.Ejercicio5b()
        print("Ej 5c\n")
        self.entrega.Ejercicio5c()
        print("Ej 5d\n")
        self.entrega.Ejercicio5d()
        print("Ej 6\n")
        self.entrega.Ejercicio6()
        print("Ej 7\n")
        self.entrega.Ejercicio7()
        print("Ej 8\n")
        self.entrega.Ejercicio8()
        print("Ej 9\n")
        self.entrega.Ejercicio9()
        print("Ej 10\n")
        self.entrega.Ejercicio10()
        print("Ej 13\n")
        self.entrega.Ejercicio13()
        pass

    def practica2(self):
        print("Ejercicio 1 práctica 2\n")
        self.entrega.Ejercicio2_1()

