import sys
default_limit = 1000
sys.setrecursionlimit(default_limit*10)
import App.logic as logic

from tabulate import tabulate

def new_logic():
    """
        Se crea una instancia del controlador
    """
    #TODO: Llamar la función de la lógica donde se crean las estructuras de datos
    return logic.new_logic()

def print_menu():
    print("Bienvenido")
    print("0- Cargar información")
    print("1- Ejecutar Requerimiento 1")
    print("2- Ejecutar Requerimiento 2")
    print("3- Ejecutar Requerimiento 3")
    print("4- Ejecutar Requerimiento 4")
    print("5- Ejecutar Requerimiento 5")
    print("6- Ejecutar Requerimiento 6")
    print("7- Salir")

def load_data(control):
    """
    Carga los datos
    """
    #TODO: Realizar la carga de datos
    print("\n1. flights_large.csv")
    print("2. flights_medium.csv")
    print("3. flights_small.csv")
    print("4. flight_test.csv\n")
    tamano_archivo = int(input("Que tamaño de archivo quieres usar? (ingresa el número): "))

    if tamano_archivo == 1:
        flights_file = "flights_large.csv"
    if tamano_archivo == 2:
        flights_file = "flights_medium.csv"
    if tamano_archivo == 3:
        flights_file = "flights_small.csv"
    if tamano_archivo == 4:
        flights_file = "flights_test.csv"

    catalog, report, tiempo_ms = logic.load_data(control, flights_file)

    resumen = [
        ["Archivo", flights_file],
        ["Registros cargados", report["total_flights"]],
        ["Tiempo de carga (ms)", round(tiempo_ms, 3)]
    ]

    print("\n=== Resumen de carga de datos de vuelos ===")
    print(tabulate(resumen, headers=["Métrica", "Valor"], tablefmt="psql"))

    print("\n=== Primeros 5 vuelos (por salida programada) ===")
    print(tabulate(report["first5"], headers="keys", tablefmt="psql"))

    print("\n=== Últimos 5 vuelos (por salida programada) ===")
    print(tabulate(report["last5"], headers="keys", tablefmt="psql"))

    return catalog


def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    reg = logic.get_data(control, id)
    if reg is None:
        print(f"No se encontró el vuelo con id = {id}")
        return

    fila = [logic.row_load_output(reg)]

    print("\n=== Información del vuelo buscado ===")
    print(tabulate(fila, headers="keys", tablefmt="psql"))

def print_req_1(control):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 1
    pass


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    pass


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    pass


def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    pass


def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    pass


def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    pass

# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 0:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 1:
            print_req_1(control)

        elif int(inputs) == 2:
            print_req_2(control)

        elif int(inputs) == 3:
            print_req_3(control)

        elif int(inputs) == 4:
            print_req_4(control)

        elif int(inputs) == 5:
            print_req_5(control)

        elif int(inputs) == 5:
            print_req_6(control)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
