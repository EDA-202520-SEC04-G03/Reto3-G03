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
    print("\n" + "="*80)
    print("Req No. 1: Identificar vuelos con retraso en salida por aerolínea")
    print("="*80)
    
    # Solicitar parámetros al usuario
    carrier_code = input("\nIngrese el código de la aerolínea (ej: UA, AA, DL): ").strip().upper()
    min_delay = float(input("Ingrese el límite inferior del rango de retraso (minutos): "))
    max_delay = float(input("Ingrese el límite superior del rango de retraso (minutos): "))
    
    # Ejecutar el requerimiento
    print("\nBuscando vuelos con retraso en salida...\n")
    result = logic.req_1(control, carrier_code, min_delay, max_delay)
    
    # Mostrar resumen
    resumen = [
        ["Aerolínea", result["carrier_code"]],
        ["Rango de retraso (min)", f"[{result['min_delay']}, {result['max_delay']}]"],
        ["Total de vuelos encontrados", result["total_flights"]],
        ["Tiempo de ejecución (ms)", round(result["exec_time_ms"], 3)]
    ]
    
    print("="*80)
    print("RESUMEN DE BÚSQUEDA")
    print("="*80)
    print(tabulate(resumen, headers=["Parámetro", "Valor"], tablefmt="psql"))
    
    # Mostrar resultados
    if result["total_flights"] > 0:
        # Mostrar primeros 5 vuelos
        if len(result["first_5"]) > 0:
            print("\n" + "="*80)
            print("PRIMEROS 5 VUELOS (menor retraso)")
            print("="*80)
            print(tabulate(result["first_5"], headers="keys", tablefmt="psql"))
        
        # Mostrar últimos 5 vuelos
        if result["total_flights"] > 5 and len(result["last_5"]) > 0:
            print("\n" + "="*80)
            print("ÚLTIMOS 5 VUELOS (mayor retraso)")
            print("="*80)
            print(tabulate(result["last_5"], headers="keys", tablefmt="psql"))
    else:
        print("\nNo se encontraron vuelos que cumplan con los criterios especificados.")
    
    print("\n" + "="*80)


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    print("\n" + "="*80)
    print("Req No. 2: Vuelos con anticipo en la llegada por aeropuerto destino")
    print("="*80)

        # Solicitar parámetros al usuario
    dest_code = input("\nIngrese el código del aeropuerto de destino (ej: JFK, LGA): ").strip().upper()
    min_early = float(input("Ingrese el límite inferior del rango de anticipo (minutos, valor positivo): "))
    max_early = float(input("Ingrese el límite superior del rango de anticipo (minutos, valor positivo): "))
    
        # Ejecutar el requerimiento
    print("\nBuscando vuelos con anticipo en la llegada...\n")
    result = logic.req_2(control, dest_code, min_early, max_early)

    # Mostrar resumen
    resumen = [
        ["Aeropuerto destino", result["dest_code"]],
        ["Rango de anticipo (min)", f"[{result['min_early']}, {result['max_early']}]"],
        ["Total de vuelos encontrados", result["total_flights"]],
        ["Tiempo de ejecución (ms)", round(result["exec_time_ms"], 3)]
    ]

    print("="*80)
    print("RESUMEN DE BÚSQUEDA")
    print("="*80)
    print(tabulate(resumen, headers=["Parámetro", "Valor"], tablefmt="psql"))

    # Mostrar resultados
    if result["total_flights"] > 0:
        # Mostrar primeros 5 vuelos
        if len(result["first_5"]) > 0:
            print("\n" + "="*80)
            print("PRIMEROS 5 VUELOS (menor anticipo en llegada)")
            print("="*80)
            print(tabulate(result["first_5"], headers="keys", tablefmt="psql"))

        # Mostrar últimos 5 vuelos
        if result["total_flights"] > 5 and len(result["last_5"]) > 0:
            print("\n" + "="*80)
            print("ÚLTIMOS 5 VUELOS (mayor anticipo en llegada)")
            print("="*80)
            print(tabulate(result["last_5"], headers="keys", tablefmt="psql"))
    else:
        print("\nNo se encontraron vuelos que cumplan con los criterios especificados.")

    print("\n" + "="*80)


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    print("\n" + "="*80)
    print("Req No. 3: Listar vuelos de una aerolínea hacia un destino en un rango de distancias")
    print("="*80)

    # Solicitar parámetros al usuario
    carrier_code = input("\nIngrese el código de la aerolínea (ej: AA, EV): ").strip().upper()
    dest_code = input("Ingrese el código del aeropuerto de destino (ej: JFK): ").strip().upper()
    min_distance = float(input("Ingrese el límite inferior del rango de distancia (millas): "))
    max_distance = float(input("Ingrese el límite superior del rango de distancia (millas): "))

    # Ejecutar el requerimiento
    print("\nBuscando vuelos que cumplan con los criterios...\n")
    result = logic.req_3(control, carrier_code, dest_code, min_distance, max_distance)

    # Mostrar resumen
    resumen = [
        ["Aerolínea", result["carrier_code"]],
        ["Aeropuerto destino", result["dest_code"]],
        ["Rango de distancias (mi)", f"[{result['min_distance']}, {result['max_distance']}]"],
        ["Total de vuelos encontrados", result["total_flights"]],
        ["Tiempo de ejecución (ms)", round(result["exec_time_ms"], 3)]
    ]

    print("="*80)
    print("RESUMEN DE BÚSQUEDA")
    print("="*80)
    print(tabulate(resumen, headers=["Parámetro", "Valor"], tablefmt="psql"))

    # Mostrar resultados
    if result["total_flights"] > 0:
        # Primeros 5 vuelos
        first_5 = result["first_5"]
        if first_5 and len(first_5) > 0:
            print("\n" + "="*80)
            print("PRIMEROS 5 VUELOS (menor distancia)")
            print("="*80)
            print(tabulate(first_5, headers="keys", tablefmt="psql"))

        # Últimos 5 vuelos
        last_5 = result["last_5"]
        if result["total_flights"] > 5 and last_5 and len(last_5) > 0:
            print("\n" + "="*80)
            print("ÚLTIMOS 5 VUELOS (mayor distancia)")
            print("="*80)
            print(tabulate(last_5, headers="keys", tablefmt="psql"))
    else:
        print("\nNo se encontraron vuelos que cumplan con los criterios especificados.")

    print("\n" + "="*80)


def print_req_4(control):
    """
    Función que imprime la solución del Requerimiento 4 en consola
    """
    print("\n" + "="*80)
    print("Req No. 4: Identificar aerolíneas con mayor número de vuelos")
    print("="*80)
    
    # Solicitar parámetros al usuario
    print("\nIngrese el rango de fechas (formato YYYY-MM-DD):")
    date_initial = input("Fecha inicial: ").strip()
    date_final = input("Fecha final: ").strip()
    
    print("\nIngrese la franja horaria de salida programada (formato HH:MM):")
    hour_initial = input("Hora inicial: ").strip()
    hour_final = input("Hora final: ").strip()
    
    top_n = int(input("\nIngrese el número de aerolíneas a mostrar (N): "))
    
    # Ejecutar el requerimiento
    print("\nBuscando aerolíneas con mayor número de vuelos...\n")
    result = logic.req_4(control, date_initial, date_final, hour_initial, hour_final, top_n)
    
    # Mostrar resumen
    resumen = [
        ["Rango de fechas", result["date_range"]],
        ["Franja horaria", result["time_range"]],
        ["Top N solicitado", result["top_n"]],
        ["Total aerolíneas encontradas", result["total_airlines"]],
        ["Tiempo de ejecución (ms)", round(result["exec_time_ms"], 3)]
    ]
    
    print("="*80)
    print("RESUMEN DE BÚSQUEDA")
    print("="*80)
    print(tabulate(resumen, headers=["Parámetro", "Valor"], tablefmt="psql"))
    
    # Mostrar resultados por aerolínea
    if len(result["airlines"]) > 0:
        for i, airline in enumerate(result["airlines"], 1):
            print("\n" + "="*80)
            print(f"AEROLÍNEA #{i}: {airline['carrier_code']} - {airline['airline_name']}")
            print("="*80)
            
            # Información general de la aerolínea
            info_general = [
                ["Código aerolínea", airline["carrier_code"]],
                ["Nombre aerolínea", airline["airline_name"]],
                ["Total vuelos", airline["total_flights"]],
                ["Duración promedio (min)", airline["avg_duration_min"]],
                ["Distancia promedio (mi)", airline["avg_distance_mi"]]
            ]
            print(tabulate(info_general, headers=["Métrica", "Valor"], tablefmt="psql"))
            
            # Información del vuelo con menor duración
            shortest = airline["shortest_flight"]
            info_shortest = [
                ["ID", shortest["id"]],
                ["Código vuelo", shortest["flight_code"]],
                ["Fecha", shortest["date"]],
                ["Hora programada salida", shortest["sched_dep_time"]],
                ["Aeropuerto origen", shortest["origin"]],
                ["Aeropuerto destino", shortest["dest"]],
                ["Duración (min)", shortest["duration_min"]]
            ]
            
            print("\nVUELO CON MENOR DURACIÓN:")
            print(tabulate(info_shortest, headers=["Campo", "Valor"], tablefmt="psql"))
    else:
        print("\nNo se encontraron aerolíneas que cumplan con los criterios especificados.")
    
    print("\n" + "="*80)


def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    print("\n" + "="*80)
    print("Req No. 5: Aerolíneas más puntuales por aeropuerto de destino")
    print("="*80)

        # Solicitar parámetros al usuario
    print("\nIngrese el rango de fechas (formato YYYY-MM-DD):")
    date_initial = input("Fecha inicial: ").strip()
    date_final = input("Fecha final: ").strip()
    
    dest_code = input("\nIngrese el código del aeropuerto de destino (ej: JFK, LGA): ").strip().upper()
    top_n = int(input("Ingrese el número de aerolíneas a mostrar (N): "))

    # Ejecutar el requerimiento
    print("\nBuscando aerolíneas más puntuales en la llegada...\n")
    result = logic.req_5(control, date_initial, date_final, dest_code, top_n)

    # Mostrar resumen
    resumen = [
        ["Rango de fechas", result["date_range"]],
        ["Aeropuerto destino", result["dest_code"]],
        ["Top N solicitado", result["top_n"]],
        ["Total aerolíneas analizadas", result["total_airlines"]],
        ["Tiempo de ejecución (ms)", round(result["exec_time_ms"], 3)]
    ]

    print("="*80)
    print("RESUMEN DE BÚSQUEDA")
    print("="*80)
    print(tabulate(resumen, headers=["Parámetro", "Valor"], tablefmt="psql"))

    # Mostrar resultados por aerolínea
    if len(result["airlines"]) > 0:
        for i, airline in enumerate(result["airlines"], 1):
            print("\n" + "="*80)
            print(f"AEROLÍNEA #{i}: {airline['carrier_code']} - {airline['airline_name']}")
            print("="*80)

            # Información general de la aerolínea
            info_general = [
                ["Código aerolínea", airline["carrier_code"]],
                ["Nombre aerolínea", airline["airline_name"]],
                ["Total vuelos", airline["total_flights"]],
                ["Duración promedio (min)", airline["avg_duration_min"]],
                ["Distancia promedio (mi)", airline["avg_distance_mi"]],
                ["Puntualidad promedio llegada (min)", airline["avg_arr_delay_min"]]
            ]
            print(tabulate(info_general, headers=["Métrica", "Valor"], tablefmt="psql"))

            # Información del vuelo con mayor distancia
            best = airline["best_distance_flight"]
            if best is not None:
                info_best = [
                    ["ID", best["id"]],
                    ["Código vuelo", best["flight_code"]],
                    ["Fecha", best["date"]],
                    ["Hora real llegada", best["arr_time_real"]],
                    ["Aeropuerto origen", best["origin"]],
                    ["Aeropuerto destino", best["dest"]],
                    ["Duración (min)", best["duration_min"]],
                    ["Distancia (mi)", best["distance_mi"]]
                ]

                print("\nVUELO CON MAYOR DISTANCIA RECORRIDA:")
                print(tabulate(info_best, headers=["Campo", "Valor"], tablefmt="psql"))
            else:
                print("\nNo se encontró un vuelo representativo para esta aerolínea.")
    else:
        print("\nNo se encontraron aerolíneas que cumplan con los criterios especificados.")

    print("\n" + "="*80)



def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    print("\n" + "="*80)
    print("Req No. 6: Aerolíneas más estables en su hora de salida")
    print("="*80)

    # Solicitar parámetros al usuario
    print("\nIngrese el rango de fechas (formato YYYY-MM-DD):")
    date_initial = input("Fecha inicial: ").strip()
    date_final = input("Fecha final: ").strip()

    print("\nIngrese el rango de distancias (en millas):")
    min_distance = float(input("Límite inferior de distancia (mi): "))
    max_distance = float(input("Límite superior de distancia (mi): "))

    top_m = int(input("\nIngrese la cantidad M de aerolíneas a mostrar: "))

    # Ejecutar el requerimiento
    print("\nBuscando aerolíneas más estables en su hora de salida...\n")
    result = logic.req_6(control, date_initial, date_final, min_distance, max_distance, top_m)

    # Mostrar resumen
    resumen = [
        ["Rango de fechas", result["date_range"]],
        ["Rango de distancias (mi)", result["distance_range"]],
        ["M solicitado", result["top_m"]],
        ["Total aerolíneas analizadas", result["total_airlines"]],
        ["Tiempo de ejecución (ms)", round(result["exec_time_ms"], 3)]
    ]

    print("="*80)
    print("RESUMEN DE BÚSQUEDA")
    print("="*80)
    print(tabulate(resumen, headers=["Parámetro", "Valor"], tablefmt="psql"))


    airlines_list = result["airlines"]["elements"]
    
    if airlines_list and len(airlines_list) > 0:
        i = 1 
        for airline in airlines_list:
            print("\n" + "="*80)
            print(f"AEROLÍNEA #{i}: {airline['carrier_code']} - {airline['airline_name']}")
            print("="*80)

            # Información general de la aerolínea
            info_general = [
                ["Código aerolínea", airline["carrier_code"]],
                ["Nombre aerolínea", airline["airline_name"]],
                ["Total vuelos analizados", airline["total_flights"]],
                ["Promedio retraso/anticipo (min)", airline["avg_dep_delay_min"]],
                ["Estabilidad salida (desv. estándar, min)", airline["std_dep_delay_min"]],
            ]
            print(tabulate(info_general, headers=["Métrica", "Valor"], tablefmt="psql"))

            # Información del vuelo más cercano al promedio
            best = airline["closest_flight"]
            if best is not None:
                info_flight = [
                    ["ID", best["id"]],
                    ["Código vuelo", best["flight_code"]],
                    ["Fecha-hora salida", best["dep_datetime"]],
                    ["Aeropuerto origen", best["origin"]],
                    ["Aeropuerto destino", best["dest"]],
                ]
                print("\nVUELO CON RETRASO/ANTICIPO MÁS CERCANO AL PROMEDIO:")
                print(tabulate(info_flight, headers=["Campo", "Valor"], tablefmt="psql"))
            else:
                print("\nNo se encontró un vuelo representativo para esta aerolínea.")

            i += 1  # incrementamos el contador
    else:
        print("\nNo se encontraron aerolíneas que cumplan con los criterios especificados.")

    print("\n" + "="*80)

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

        elif int(inputs) == 6:
            print_req_6(control)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
