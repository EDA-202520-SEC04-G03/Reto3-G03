import time as pytime
import csv
import os
from datetime import datetime, date, time as dtime, timedelta

import DataStructures.List.array_list as al
import DataStructures.List.single_linked_list as sll
import DataStructures.Map.map_separate_chaining as mp
import DataStructures.Map.map_entry as me
import DataStructures.Tree.binary_search_tree as bst
import DataStructures.Tree.red_black_tree as rbt 
import DataStructures.Priority_queue.priority_queue as pq

csv.field_size_limit(2147483647)

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Data", "Challenge-3")

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    
    flights = rbt.new_map()
    catalog = {
        "flights": flights
    }
    return catalog

# Funciones para la carga de datos

def safe_str(x):
    if x is None:
        return "Unknown"
    s = str(x).strip()
    return s if s else "Unknown"


def safe_float(x):
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    neg = (s[0] == '-')
    pos = (s[0] == '+')
    digits = s[1:] if (neg or pos) else s
    if digits.count('.') <= 1 and digits.replace('.', '', 1).isdigit():
        # En este punto float() no debería fallar
        return float(s)
    return None


def is_leap(y):
    return (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)

def days_in_month(y, m):
    if m < 1 or m > 12:
        return 0
    if m in (1,3,5,7,8,10,12):
        return 31
    if m in (4,6,9,11):
        return 30
    # Es febrero
    return 29 if is_leap(y) else 28

def valid_ymd(y, m, d):
    if y < 1 or m < 1 or m > 12:
        return False
    dim = days_in_month(y, m)
    return 1 <= d <= dim

def parse_date(s):
    if s is None:
        return None
    ss = str(s).strip()
    if not ss:
        return None

    y = m = d = None

    if '-' in ss:
        parts = ss.split('-')
        if len(parts) == 3 and len(parts[0]) == 4 and parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit():
            y = int(parts[0]); m = int(parts[1]); d = int(parts[2])
        else:
            return None
    elif '/' in ss:
        parts = ss.split('/')
        if len(parts) != 3 or not (parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit()):
            return None
        # Detectar si es YYYY/MM/DD o DD/MM/YYYY
        if len(parts[0]) == 4:
            # YYYY/MM/DD
            y = int(parts[0]); m = int(parts[1]); d = int(parts[2])
        else:
            # DD/MM/YYYY
            d = int(parts[0]); m = int(parts[1]); y = int(parts[2])
    else:
        return None

    if not valid_ymd(y, m, d):
        return None

    return date(y, m, d)


def parse_hhmm(s):
    if s is None:
        return None
    ss = str(s).strip()
    if not ss:
        return None

    # Caso HH:MM
    if ":" in ss:
        parts = ss.split(":")
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            hh = int(parts[0]); mm = int(parts[1])
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return dtime(hour=hh, minute=mm)
        return None

    # Caso HHMM (p.ej., "630" -> 06:30)
    if ss.isdigit():
        val = int(ss)
        hh, mm = (val // 100), (val % 100)
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return dtime(hour=hh, minute=mm)

    return None


def combine_dt(d, t):
    if d is None or t is None:
        return None
    return datetime(d.year, d.month, d.day, t.hour, t.minute, 0)


def to_ts(dt):
    if dt is None:
        return None
    return int(dt.timestamp())


def minutes_between(a, b):
    if a is None or b is None:
        return None
    return (b - a).total_seconds() / 60.0

def fmt_hhmm(dt_obj):
    if dt_obj is None:
        return "Unknown"
    return dt_obj.strftime("%H:%M")


def fmt_date(d_obj):
    if d_obj is None:
        return "Unknown"
    return f"{d_obj.year:04d}-{d_obj.month:02d}-{d_obj.day:02d}"


def row_load_output(reg):
    dur = reg.get("duration_min")
    dist = reg.get("distance_mi")
    return {
        "date": fmt_date(reg.get("date")),
        "dep_time_real": fmt_hhmm(reg.get("dep_dt")),
        "arr_time_real": fmt_hhmm(reg.get("arr_dt")),
        "carrier_code": reg.get("carrier") or "Unknown",
        "airline_name": reg.get("airline_name") or "Unknown",
        "aircraft_id": reg.get("aircraft_id") or "Unknown",
        "origin": reg.get("origin") or "Unknown",
        "dest": reg.get("dest") or "Unknown",
        "duration_min": "Unknown" if dur is None else round(dur, 2),
        "distance_mi": "Unknown" if dist is None else round(dist, 2),
    }

def inorder_first_k(node, k, acc):
    if node is None or len(acc) >= k:
        return
    inorder_first_k(node["left"], k, acc)
    if len(acc) < k:
        acc.append(node["value"]) 
    inorder_first_k(node["right"], k, acc)


def inorder_last_k(node, k, acc):
    if node is None or len(acc) >= k:
        return
    inorder_last_k(node["right"], k, acc)
    if len(acc) < k:
        acc.append(node["value"]) 
    inorder_last_k(node["left"], k, acc)



def get_load_report(catalog, k, load_time):
    total = rbt.size(catalog["flights"]) 
    first_k, last_k = [], []
    root = catalog["flights"]["root"]
    inorder_first_k(root, k, first_k)
    inorder_last_k(root, k, last_k)
    last_k.reverse()

    first5_out = [row_load_output(reg) for reg in first_k]
    last5_out = [row_load_output(reg) for reg in last_k]

    return {
        "load_time_ms": load_time,
        "total_flights": total,
        "first5": first5_out,
        "last5": last5_out,
    }


def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos

    start = get_time()

    path = os.path.join(data_dir, filename)
    file = open(path, encoding="utf-8")
    reader = csv.DictReader(file)
    for row in reader:
        # Normalización de los datos
        flight_id   = safe_str(row.get("id"))
        flight_code = safe_str(row.get("flight"))
        carrier     = safe_str(row.get("carrier"))
        airline_nm  = safe_str(row.get("name"))
        aircraft_id = safe_str(row.get("tailnum"))
        origin      = safe_str(row.get("origin"))
        dest        = safe_str(row.get("dest"))
        date_str = row.get("date")
        flight_date = parse_date(date_str)

        dep_prog = parse_hhmm(row.get("sched_dep_time"))
        arr_prog = parse_hhmm(row.get("sched_arr_time"))
        dep_real = parse_hhmm(row.get("dep_time"))
        arr_real = parse_hhmm(row.get("arr_time"))

        sched_dep_dt = combine_dt(flight_date, dep_prog)
        sched_arr_dt = combine_dt(flight_date, arr_prog)
        dep_dt       = combine_dt(flight_date, dep_real)
        arr_dt       = combine_dt(flight_date, arr_real)

        if dep_dt is not None and arr_dt is not None and arr_dt < dep_dt:
            arr_dt = arr_dt + timedelta(days=1)
        if sched_dep_dt is not None and sched_arr_dt is not None and sched_arr_dt < sched_dep_dt:
            sched_arr_dt = sched_arr_dt + timedelta(days=1)

        sched_dep_ts = to_ts(sched_dep_dt)
        real_dep_ts  = to_ts(dep_dt)
        sched_arr_ts = to_ts(sched_arr_dt)
        real_arr_ts  = to_ts(arr_dt)

        dep_delay_min = minutes_between(sched_dep_dt, dep_dt)
        arr_delay_min = minutes_between(sched_arr_dt, arr_dt)
        duration_min  = minutes_between(dep_dt, arr_dt)
        distance_mi   = safe_float(row.get("distance"))

        sched_dep_hour = sched_dep_dt.hour if sched_dep_dt is not None else None

        reg = {
            "id": flight_id,
            "flight_code": flight_code,
            "carrier": carrier,
            "airline_name": airline_nm,
            "aircraft_id": aircraft_id,
            "origin": origin,
            "dest": dest,
            "date": flight_date,
            "sched_dep_dt": sched_dep_dt,
            "sched_arr_dt": sched_arr_dt,
            "dep_dt": dep_dt,
            "arr_dt": arr_dt,
            "sched_dep_ts": sched_dep_ts,
            "real_dep_ts": real_dep_ts,
            "sched_arr_ts": sched_arr_ts,
            "real_arr_ts": real_arr_ts,
            "dep_delay_min": dep_delay_min,
            "arr_delay_min": arr_delay_min,
            "duration_min": duration_min,
            "distance_mi": distance_mi,
            "sched_dep_hour": sched_dep_hour,
        }

        # Insertar en RBT maestro con clave (sched_dep_ts, id)
        if (sched_dep_ts is not None) and (flight_id != "Unknown"):
            rbt.put(catalog["flights"], (sched_dep_ts, flight_id), reg)

    end = get_time()
    load_ms = delta_time(start, end)
    k = 5 
    report = get_load_report(catalog, k, load_ms)

    return catalog, report, load_ms

# Funciones de consulta sobre el catálogo

def get_data(catalog, id):
    """
    get_data sacado de las implementaciones de retos anteriores
    """
    def find_by_flight_id_node(node, flight_id):
        if node is None:
            return None
        # Buscar en el subárbol izquierdo
        res = find_by_flight_id_node(node["left"], flight_id)
        if res is not None:
            return res
        # Revisar este nodo
        val = node["value"]
        if val and val.get("id") == flight_id:
            return val
        # Buscar en el subárbol derecho
        return find_by_flight_id_node(node["right"], flight_id)

    root = catalog["flights"]["root"]
    return find_by_flight_id_node(root, id)


def req_1(catalog, carrier_code, min_delay, max_delay):
    """
    Retorna el resultado del requerimiento 1
    Identifica vuelos con retraso en salida dentro de un rango específico para una aerolínea
    """
    
    
    start = get_time()
    
    # Lista para almacenar vuelos que cumplen los criterios
    matching_flights = al.new_list()
    
    # Recorrer todos los vuelos del árbol
    def traverse_tree(node):
        if node is None:
            return
        
        # Recorrer subárbol izquierdo
        traverse_tree(node["left"])
        
        # Procesar nodo actual
        flight = node["value"]
        if flight is not None:
            # Verificar que sea de la aerolínea correcta
            if flight.get("carrier") == carrier_code:
                dep_delay = flight.get("dep_delay_min")
                
                # Verificar que el retraso esté en el rango
                if dep_delay is not None and min_delay <= dep_delay <= max_delay:
                    al.add_last(matching_flights, flight)
        
        # Recorrer subárbol derecho
        traverse_tree(node["right"])
    
    # Iniciar recorrido desde la raíz
    traverse_tree(catalog["flights"]["root"])
    
    # Función de ordenamiento definida explícitamente
    def sort_key_req1(f1, f2):
        """
        Llave de ordenamiento para req_1:
        - Primero por retraso de salida (ascendente)
        - Luego por fecha/hora real de salida
        Retorna True si f1 debe ir antes que f2
        """
        dep_delay1 = f1.get("dep_delay_min")
        dep_delay2 = f2.get("dep_delay_min")
        
        if dep_delay1 is None:
            delay_val1 = float('inf')
        else:
            delay_val1 = dep_delay1
            
        if dep_delay2 is None:
            delay_val2 = float('inf')
        else:
            delay_val2 = dep_delay2
        
        # Comparar por retraso primero
        if delay_val1 != delay_val2:
            return delay_val1 < delay_val2
        
        # Si el retraso es igual, comparar por fecha/hora de salida
        dep_dt1 = f1.get("dep_dt")
        dep_dt2 = f2.get("dep_dt")
        
        if dep_dt1 is None:
            dt_val1 = datetime.max
        else:
            dt_val1 = dep_dt1
            
        if dep_dt2 is None:
            dt_val2 = datetime.max
        else:
            dt_val2 = dep_dt2
        
        return dt_val1 < dt_val2
    
    # Ordenar usando el método de array_list (puedes elegir el algoritmo)
    matching_flights = al.merge_sort(matching_flights, sort_key_req1)
    
    end = get_time()
    exec_time = delta_time(start, end)
    
    total_flights = al.size(matching_flights)
    
    # Preparar vuelos para mostrar (primeros 5 y últimos 5)
    first_5 = []
    last_5 = []
    
    for i in range(min(5, total_flights)):
        first_5.append(al.get_element(matching_flights, i))
    
    for i in range(max(0, total_flights - 5), total_flights):
        last_5.append(al.get_element(matching_flights, i))
    
    # Formatear vuelos para salida
    def format_flight(flight):
        dep_delay = flight.get("dep_delay_min")
        return {
            "id": flight.get("id") or "Unknown",
            "flight_code": flight.get("flight_code") or "Unknown",
            "date": fmt_date(flight.get("date")),
            "carrier": flight.get("carrier") or "Unknown",
            "airline_name": flight.get("airline_name") or "Unknown",
            "origin": flight.get("origin") or "Unknown",
            "dest": flight.get("dest") or "Unknown",
            "sched_dep_time": fmt_hhmm(flight.get("sched_dep_dt")),
            "dep_time_real": fmt_hhmm(flight.get("dep_dt")),
            "dep_delay_min": "Unknown" if dep_delay is None else round(dep_delay, 2)
        }
    
    first_5_formatted = [format_flight(f) for f in first_5]
    last_5_formatted = [format_flight(f) for f in last_5]
    
    return {
        "exec_time_ms": exec_time,
        "carrier_code": carrier_code,
        "min_delay": min_delay,
        "max_delay": max_delay,
        "total_flights": total_flights,
        "first_5": first_5_formatted,
        "last_5": last_5_formatted
    }



def req_2(catalog, dest_code, min_early, max_early):
    
    start = get_time()

    # Asegurar que min_early <= max_early
    if min_early > max_early:
        tmp = min_early
        min_early = max_early
        max_early = tmp

    # Convertimos el rango de anticipo positivo a rango de "delay" negativo
    
    delay_min = -max_early
    delay_max = -min_early

    matching_flights = al.new_list()

    def traverse(node):
        if node is None:
            return

        traverse(node["left"])

        flight = node["value"]
        if flight is not None:
            dest = flight.get("dest")
            arr_delay = flight.get("arr_delay_min")

            if dest == dest_code and arr_delay is not None:
                # Solo anticipos (valores negativos) dentro del rango
                if arr_delay < 0 and delay_min <= arr_delay <= delay_max:
                    al.add_last(matching_flights, flight)

        traverse(node["right"])

    traverse(catalog["flights"]["root"])

    # Comparator function for merge_sort
    def sort_key_req2(f1, f2):
        arr_delay1 = f1.get("arr_delay_min")
        arr_delay2 = f2.get("arr_delay_min")

        if arr_delay1 is None:
            anticip1 = float("inf")
        else:
            if arr_delay1 < 0:
                anticip1 = -arr_delay1
            else:
                anticip1 = float("inf")

        if arr_delay2 is None:
            anticip2 = float("inf")
        else:
            if arr_delay2 < 0:
                anticip2 = -arr_delay2
            else:
                anticip2 = float("inf")

        if anticip1 != anticip2:
            return anticip1 < anticip2

        arr_dt1 = f1.get("arr_dt")
        arr_dt2 = f2.get("arr_dt")
        if arr_dt1 is None:
            arr_dt1 = datetime.max
        if arr_dt2 is None:
            arr_dt2 = datetime.max
        return arr_dt1 < arr_dt2

    # Ordenar usando merge_sort de array_list porque es el más god 
    matching_flights = al.merge_sort(matching_flights, sort_key_req2)

    end = get_time()
    exec_time = delta_time(start, end)
    total_flights = al.size(matching_flights)

    # Primeros 5 y últimos 5 usando array_list
    first_5 = al.new_list()
    last_5 = al.new_list()
    for i in range(min(5, total_flights)):
        al.add_last(first_5, al.get_element(matching_flights, i))
    for i in range(max(0, total_flights - 5), total_flights):
        al.add_last(last_5, al.get_element(matching_flights, i))

    def format_flight(flight):
        arr_delay = flight.get("arr_delay_min")
        if arr_delay is not None and arr_delay < 0:
            anticip = -arr_delay
        else:
            anticip = None

        return {
            "id": flight.get("id") or "Unknown",
            "flight_code": flight.get("flight_code") or "Unknown",
            "date": fmt_date(flight.get("date")),
            "carrier": flight.get("carrier") or "Unknown",
            "airline_name": flight.get("airline_name") or "Unknown",
            "origin": flight.get("origin") or "Unknown",
            "dest": flight.get("dest") or "Unknown",
            "sched_arr_time": fmt_hhmm(flight.get("sched_arr_dt")),
            "arr_time_real": fmt_hhmm(flight.get("arr_dt")),
            "early_min": "Unknown" if anticip is None else round(anticip, 2)
        }

    first_5_formatted = []
    for i in range(al.size(first_5)):
        first_5_formatted.append(format_flight(al.get_element(first_5, i)))

    last_5_formatted = []
    for i in range(al.size(last_5)):
        last_5_formatted.append(format_flight(al.get_element(last_5, i)))

    result = {
        "exec_time_ms": exec_time,
        "dest_code": dest_code,
        "min_early": min_early,
        "max_early": max_early,
        "total_flights": total_flights,
        "first_5": first_5_formatted,
        "last_5": last_5_formatted
    }

    return result


def req_3(catalog, carrier_code, dest_code, min_distance, max_distance):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    start = get_time()

    # Asegurar que min_distance <= max_distance
    if min_distance > max_distance:
        tmp = min_distance
        min_distance = max_distance
        max_distance = tmp

    # Lista array_list donde guardaremos los vuelos que cumplen
    matching_flights = al.new_list()


    # Obtenemos TODOS los valores del RBT en orden
    all_values_list = rbt.values(catalog["flights"], None, None)

    node = all_values_list["first"]
    while node is not None:
        flight = node["info"]

        if flight is not None:
            carrier = flight["carrier"]
            dest = flight["dest"]
            dist = flight["distance_mi"]

            # Asumimos que dist ya es float o None
            if carrier == carrier_code and dest == dest_code and dist != "Unknown" and min_distance <= dist <= max_distance:
                al.add_last(matching_flights, flight)

        node = node["next"]
    # Ordenar usando quick_sort
    # Definimos sort_criteria para req3
    def sort_criteria_req3(f1, f2):
        d1 = f1["distance_mi"]
        d2 = f2["distance_mi"]

        if (d1 == "Unknown" and d2 == "Unknown") or (d1 == d2):
            # Ninguna tiene distancia válida, pasamos a comparar por fecha
            t1 = f1["arr_dt"]
            t2 = f2["arr_dt"]

            # Si tampoco hay fechas, no imponemos orden
            if t1 == "Unknown" and t2 == "Unknown":
                return False
            if t1 == "Unknown":
                return False    # f1 no tiene fecha va después
            if t2 == "Unknown":
                return True     # f1 tiene fecha y f2 no f1 antes

            return t1 < t2

        if d1 == "Unknown":
            # f1 no tiene distancia, f2 sí f1 debe ir DESPUÉS de f2
            return False

        if d2 == "Unknown":
            # f2 no tiene distancia, f1 sí f1 debe ir ANTES de f2
            return True

        if d1 < d2:
            return True
        if d1 > d2:
            return False
        
        return False


    matching_flights = al.quick_sort(matching_flights, sort_crit=sort_criteria_req3)

    end = get_time()
    exec_time = delta_time(start, end)

    total_flights = al.size(matching_flights)

    if total_flights > 10:
        first_5 = al.sub_list(matching_flights, 0, 5)["elements"]
        last_5 = al.sub_list(matching_flights, total_flights - 5, 5)["elements"]
    else:
        first_5 = matching_flights["elements"]
        last_5 = matching_flights["elements"]

    def format_flight(flight):
        dist = flight["distance_mi"]
        return {
            "id": flight["id"],
            "flight_code": flight["flight_code"],
            "date": fmt_date(flight["date"]),
            "airline_name": flight["airline_name"],
            "carrier": flight["carrier"],
            "origin": flight["origin"],
            "dest": flight["dest"],
            "distance_mi": dist
        }

    first_5_formatted = al.new_list()
    last_5_formatted = al.new_list()

    for f in first_5:
        al.add_last(first_5_formatted, format_flight(f))
    for f in last_5:
        al.add_last(last_5_formatted, format_flight(f))

    return {
        "exec_time_ms": exec_time,
        "carrier_code": carrier_code,
        "dest_code": dest_code,
        "min_distance": min_distance,
        "max_distance": max_distance,
        "total_flights": total_flights,
        "first_5": first_5_formatted,
        "last_5": last_5_formatted
    }


def req_4(catalog, date_initial, date_final, hour_initial, hour_final, top_n):
    """
    Retorna el resultado del requerimiento 4
    Identifica las N aerolíneas con mayor número de vuelos en un rango de fechas
    y franja horaria, y obtiene el vuelo con menor duración de cada una.
    """
    import DataStructures.List.array_list as al
    
    start = get_time()
    
    # Parsear las fechas y horas de entrada
    fecha_ini = parse_date(date_initial)
    fecha_fin = parse_date(date_final)
    hora_ini = parse_hhmm(hour_initial)
    hora_fin = parse_hhmm(hour_final)
    
    # Diccionario para contar vuelos por aerolínea
    airlines_data = {}
    
    # Recorrer todos los vuelos del árbol
    def traverse_tree(node):
        if node is None:
            return
        
        # Recorrer subárbol izquierdo
        traverse_tree(node["left"])
        
        # Procesar nodo actual
        flight = node["value"]
        if flight is not None:
            flight_date = flight.get("date")
            sched_dep_dt = flight.get("sched_dep_dt")
            carrier = flight.get("carrier")
            duration = flight.get("duration_min")
            
            # Verificar que tengamos todos los datos necesarios
            if (flight_date is not None and sched_dep_dt is not None and 
                carrier is not None and carrier != "Unknown" and
                duration is not None):
                
                # Verificar que la fecha esté en el rango
                if fecha_ini <= flight_date <= fecha_fin:
                    # Obtener la hora programada de salida
                    sched_hour = sched_dep_dt.hour
                    sched_minute = sched_dep_dt.minute
                    
                    # Crear objeto time para comparar
                    flight_time = dtime(hour=sched_hour, minute=sched_minute)
                    
                    # Verificar que la hora esté en la franja
                    if hora_ini <= flight_time <= hora_fin:
                        # Si la aerolínea no está en el diccionario, inicializarla
                        if carrier not in airlines_data:
                            airlines_data[carrier] = {
                                "carrier_code": carrier,
                                "airline_name": flight.get("airline_name") or "Unknown",
                                "total_flights": 0,
                                "total_duration": 0.0,
                                "total_distance": 0.0,
                                "shortest_flight": None
                            }
                        
                        # Incrementar contador de vuelos
                        airlines_data[carrier]["total_flights"] += 1
                        
                        # Acumular duración
                        airlines_data[carrier]["total_duration"] += duration
                        
                        # Acumular distancia si existe
                        distance = flight.get("distance_mi")
                        if distance is not None:
                            airlines_data[carrier]["total_distance"] += distance
                        
                        # Actualizar vuelo con menor duración
                        current_shortest = airlines_data[carrier]["shortest_flight"]
                        if current_shortest is None:
                            airlines_data[carrier]["shortest_flight"] = flight
                        else:
                            current_duration = current_shortest.get("duration_min")
                            if duration < current_duration:
                                airlines_data[carrier]["shortest_flight"] = flight
                            elif duration == current_duration:
                                # Desempate por fecha-hora programada de salida
                                if sched_dep_dt < current_shortest.get("sched_dep_dt"):
                                    airlines_data[carrier]["shortest_flight"] = flight
        
        # Recorrer subárbol derecho
        traverse_tree(node["right"])
    
    # Iniciar recorrido desde la raíz
    traverse_tree(catalog["flights"]["root"])
    
    # Convertir diccionario a array_list
    airlines_list = al.new_list()
    for carrier_code in airlines_data:
        al.add_last(airlines_list, airlines_data[carrier_code])
    
    # Función de ordenamiento definida explícitamente
    def sort_key_req4(a1, a2):
        """
        Llave de ordenamiento para req_4:
        - Primero por número total de vuelos (descendente)
        - Luego alfabéticamente por código de aerolínea
        Retorna True si a1 debe ir antes que a2
        """
        total_flights1 = a1["total_flights"]
        total_flights2 = a2["total_flights"]
        
        # Comparar por total de vuelos (descendente)
        if total_flights1 != total_flights2:
            return total_flights1 > total_flights2
        
        # Si son iguales, comparar alfabéticamente por código
        carrier_code1 = a1["carrier_code"]
        carrier_code2 = a2["carrier_code"]
        
        return carrier_code1 < carrier_code2
    
    # Ordenar usando el método de array_list
    airlines_list = al.merge_sort(airlines_list, sort_key_req4)
    
    # Tomar solo las top N aerolíneas
    top_airlines = []
    for i in range(min(top_n, al.size(airlines_list))):
        airline = al.get_element(airlines_list, i)
        
        # Calcular promedios
        total_flights = airline["total_flights"]
        if total_flights > 0:
            airline["avg_duration"] = airline["total_duration"] / total_flights
            airline["avg_distance"] = airline["total_distance"] / total_flights
        else:
            airline["avg_duration"] = 0.0
            airline["avg_distance"] = 0.0
        
        top_airlines.append(airline)
    
    end = get_time()
    exec_time = delta_time(start, end)
    
    # Formatear resultado
    def format_airline(airline):
        shortest = airline["shortest_flight"]
        return {
            "carrier_code": airline["carrier_code"],
            "airline_name": airline["airline_name"],
            "total_flights": airline["total_flights"],
            "avg_duration_min": round(airline["avg_duration"], 2),
            "avg_distance_mi": round(airline["avg_distance"], 2),
            "shortest_flight": {
                "id": shortest.get("id") or "Unknown",
                "flight_code": shortest.get("flight_code") or "Unknown",
                "date": fmt_date(shortest.get("date")),
                "sched_dep_time": fmt_hhmm(shortest.get("sched_dep_dt")),
                "origin": shortest.get("origin") or "Unknown",
                "dest": shortest.get("dest") or "Unknown",
                "duration_min": round(shortest.get("duration_min"), 2)
            }
        }
    
    formatted_airlines = [format_airline(a) for a in top_airlines]
    
    return {
        "exec_time_ms": exec_time,
        "date_range": f"{date_initial} a {date_final}",
        "time_range": f"{hour_initial} a {hour_final}",
        "top_n": top_n,
        "total_airlines": al.size(airlines_list),
        "airlines": formatted_airlines
    }


def req_5(catalog, date_initial, date_final, dest_code, top_n):
    
    start = get_time()

    fecha_ini = parse_date(date_initial)
    fecha_fin = parse_date(date_final)

    airlines_data = mp.new_map(2048, 0.8)

    def traverse(node):
        if node is None:
            return

        traverse(node["left"])

        flight = node["value"]
        if flight is not None:
            flight_date = flight.get("date")
            dest = flight.get("dest")
            carrier = flight.get("carrier")
            arr_delay = flight.get("arr_delay_min")

            if (flight_date is not None and
                carrier is not None and carrier != "Unknown" and
                dest == dest_code and
                arr_delay is not None and
                fecha_ini <= flight_date <= fecha_fin):

                entry = mp.get(airlines_data, carrier)
                if entry is None:
                    info = {
                        "carrier_code": carrier,
                        "airline_name": flight.get("airline_name") or "Unknown",
                        "total_flights": 0,
                        "total_duration": 0.0,
                        "total_distance": 0.0,
                        "sum_arr_delay": 0.0,
                        "count_delay": 0,
                        "max_distance_flight": None
                    }
                    mp.put(airlines_data, carrier, info)
                else:
                    info = entry

                info["total_flights"] += 1

                duration = flight.get("duration_min")
                if duration is not None:
                    info["total_duration"] += duration

                distance = flight.get("distance_mi")
                if distance is not None:
                    info["total_distance"] += distance

                info["sum_arr_delay"] += arr_delay
                info["count_delay"] += 1

                # Actualizar vuelo con mayor distancia
                current_best = info["max_distance_flight"]
                if distance is not None:
                    if current_best is None:
                        info["max_distance_flight"] = flight
                    else:
                        best_dist = current_best.get("distance_mi")
                        if best_dist is None or distance > best_dist:
                            info["max_distance_flight"] = flight
                        elif distance == best_dist:
                            current_dt = current_best.get("arr_dt")
                            new_dt = flight.get("arr_dt")
                            if current_dt is not None and new_dt is not None:
                                if new_dt < current_dt:
                                    info["max_distance_flight"] = flight

        traverse(node["right"])

    traverse(catalog["flights"]["root"])

    airlines_list = al.new_list()

    airlines_values = mp.value_set(airlines_data)
    n_airlines = al.size(airlines_values)
    for i in range(n_airlines):
        info = al.get_element(airlines_values, i)
        if info["count_delay"] == 0:
            continue

        avg_delay = info["sum_arr_delay"] / info["count_delay"]

        if info["total_flights"] > 0:
            avg_duration = info["total_duration"] / info["total_flights"]
            avg_distance = info["total_distance"] / info["total_flights"]
        else:
            avg_duration = 0.0
            avg_distance = 0.0

        info["avg_arr_delay_min"] = avg_delay
        info["avg_duration_min"] = avg_duration
        info["avg_distance_mi"] = avg_distance

        al.add_last(airlines_list, info)

    # Comparator for merge_sort
    def sort_key_req5(a1, a2):
        avg_delay_val1 = a1["avg_arr_delay_min"]
        avg_delay_val2 = a2["avg_arr_delay_min"]
        # Usamos el valor absoluto como “grado de puntualidad”
        val1 = -avg_delay_val1 if avg_delay_val1 < 0 else avg_delay_val1
        val2 = -avg_delay_val2 if avg_delay_val2 < 0 else avg_delay_val2
        if val1 != val2:
            return val1 < val2
        return a1["carrier_code"] < a2["carrier_code"]

    airlines_list = al.merge_sort(airlines_list, sort_key_req5)

    # Top N, usando array_list
    top_airlines = al.new_list()
    n = min(top_n, al.size(airlines_list))
    for i in range(n):
        al.add_last(top_airlines, al.get_element(airlines_list, i))

    end = get_time()
    exec_time = delta_time(start, end)

    def format_airline(info):
        flight = info["max_distance_flight"]
        if flight is not None:
            flight_block = {
                "id": flight.get("id") or "Unknown",
                "flight_code": flight.get("flight_code") or "Unknown",
                "date": fmt_date(flight.get("date")),
                "arr_time_real": fmt_hhmm(flight.get("arr_dt")),
                "origin": flight.get("origin") or "Unknown",
                "dest": flight.get("dest") or "Unknown",
                "duration_min": "Unknown" if flight.get("duration_min") is None else round(flight.get("duration_min"), 2),
                "distance_mi": "Unknown" if flight.get("distance_mi") is None else round(flight.get("distance_mi"), 2)
            }
        else:
            flight_block = None

        return {
            "carrier_code": info["carrier_code"],
            "airline_name": info["airline_name"],
            "total_flights": info["total_flights"],
            "avg_duration_min": round(info["avg_duration_min"], 2),
            "avg_distance_mi": round(info["avg_distance_mi"], 2),
            "avg_arr_delay_min": round(info["avg_arr_delay_min"], 2),
            "best_distance_flight": flight_block
        }

    formatted_airlines = []
    for i in range(al.size(top_airlines)):
        formatted_airlines.append(format_airline(al.get_element(top_airlines, i)))

    result = {
        "exec_time_ms": exec_time,
        "date_range": f"{date_initial} a {date_final}",
        "dest_code": dest_code,
        "top_n": top_n,
        "total_airlines": al.size(airlines_list),
        "airlines": formatted_airlines
    }

    return result

def req_6(catalog, date_initial, date_final, min_distance, max_distance, top_m):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    start = get_time()

    # Parsear fechas
    fecha_ini = parse_date(date_initial)
    fecha_fin = parse_date(date_final)

    # Si las fechas vienen invertidas, corregir
    if fecha_ini != "Unknown" and fecha_fin != "Unknown" and fecha_ini > fecha_fin:
        tmp_fecha = fecha_ini
        fecha_ini = fecha_fin
        fecha_fin = tmp_fecha

    # Asegurar que min_distance <= max_distance
    if min_distance > max_distance:
        tmp = min_distance
        min_distance = max_distance
        max_distance = tmp

    airlines_data = mp.new_map(1024, 0.8)

    # Aca creamos un ssl con los values de los vuelos    
    all_values_list = rbt.values(catalog["flights"], None, None)
    node = all_values_list["first"]
    while node is not None:
        flight = node["info"]

        flight_date = flight["date"]
        dist = flight["distance_mi"]
        carrier = flight["carrier"]
        delay = flight["dep_delay_min"]

        if (flight_date != "Unknown" and carrier != "Unknown" and dist != "Unknown" and delay != "Unknown" and fecha_ini != "Unknown" and fecha_fin != "Unknown" and fecha_ini <= flight_date <= fecha_fin and min_distance <= dist <= max_distance):

            entry = mp.get(airlines_data, carrier)

            if entry is None:
                info = {
                    "carrier_code": carrier,
                    "airline_name": flight["airline_name"],
                    "delays": al.new_list(),
                    "flights": al.new_list()
                }
                mp.put(airlines_data, carrier, info)
            else:
                info = entry

            al.add_last(info["delays"], delay)
            al.add_last(info["flights"], flight)

        node = node["next"]

    # Construimos una lista de aerolíneas con sus métricas
    airlines_values = mp.value_set(airlines_data)  
    size_vals = al.size(airlines_values)

    airlines_list = al.new_list()

    for i in range(0, size_vals):
        info = al.get_element(airlines_values, i)  
        carrier_code = info["carrier_code"]

        delays = info["delays"]
        n = al.size(delays)

        if n != 0:
            # Cálculo de promedio y desviación estándar
            sum_delay = 0.0
            sum_sq = 0.0
            for j in range(0, n):  
                d = al.get_element(delays, j)
                sum_delay += d
                sum_sq += d ** 2

            avg_delay = sum_delay / n
            variance = (sum_sq / n) - (avg_delay * avg_delay)
            if variance < 0:
                variance = 0.0
            std_delay = variance ** 0.5

            # Encontrar el vuelo con retraso más cercano al promedio
            best_flight = None
            best_diff = None

            f = al.size(info["flights"])
            for j in range(0, f):  
                flight = al.get_element(info["flights"], j)
                d = flight["dep_delay_min"]
                if d != "Unknown":
                    diff = abs(d - avg_delay)
                    if best_diff is None:
                        best_diff = diff
                        best_flight = flight
                    else:
                        if diff < best_diff:
                            best_diff = diff
                            best_flight = flight
                        elif diff == best_diff:
                            # Desempate: fecha-hora real de salida más temprana
                            dep_dt = flight["dep_dt"]
                            if best_flight is not None:
                                best_dep_dt = best_flight["dep_dt"]
                            else:
                                best_dep_dt = None
                            if (
                                dep_dt != "Unknown"
                                and best_dep_dt is not None
                                and dep_dt < best_dep_dt
                            ):
                                best_flight = flight

            al.add_last(airlines_list, {
                "carrier_code": carrier_code,
                "airline_name": info["airline_name"],
                "total_flights": n,
                "avg_delay": avg_delay,
                "std_delay": std_delay,
                "best_flight": best_flight
            })

    # Ordenar por estabilidad (desviación estándar ascendente),
    # luego por promedio de retraso ascendente
    def sort_airlines(a, b):
    # Primer criterio: std_delay
        if a["std_delay"] < b["std_delay"]:
            return True
        if a["std_delay"] > b["std_delay"]:
            return False

    # Empate: segundo criterio: avg_delay
        return a["avg_delay"] < b["avg_delay"]

    al.quick_sort(airlines_list, sort_crit=sort_airlines)

    # Tomar el top de M más estables
    if al.size(airlines_list) < top_m:
        top_airlines = airlines_list
    else:
        top_airlines = al.sub_list(airlines_list, 0, top_m)

    end = get_time()
    exec_time = delta_time(start, end)

    def format_airline(a):
        flight = a["best_flight"]
        if flight is not None:
            dep_dt = flight["dep_dt"]
            flight_block = {
                "id": flight["id"],
                "flight_code": flight["flight_code"],
                "dep_datetime": fmt_date(dep_dt),
                "origin": flight["origin"],
                "dest": flight["dest"],
            }
        else:
            flight_block = None

        return {
            "carrier_code": a["carrier_code"],
            "airline_name": a["airline_name"],
            "total_flights": a["total_flights"],
            "avg_dep_delay_min": a["avg_delay"],
            "std_dep_delay_min": a["std_delay"],
            "closest_flight": flight_block
        }

    formatted_airlines = al.new_list()
    for a in top_airlines["elements"]:
        al.add_last(formatted_airlines, format_airline(a))

    return {
        "exec_time_ms": exec_time,
        "date_range": f"{date_initial} a {date_final}",
        "distance_range": f"{min_distance} a {max_distance}",
        "top_m": top_m,
        "total_airlines": al.size(airlines_list),
        "airlines": formatted_airlines
    }



# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(pytime.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
