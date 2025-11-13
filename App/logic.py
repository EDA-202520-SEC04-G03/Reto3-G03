import time as pytime
import csv
import os
from datetime import datetime, date, time as dtime, timedelta

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
    
    Args:
        catalog: Catálogo con los datos de vuelos
        carrier_code: Código de la aerolínea (ej: "UA")
        min_delay: Límite inferior del rango de retraso en minutos
        max_delay: Límite superior del rango de retraso en minutos
    
    Returns:
        dict con los resultados del requerimiento
    """
    start = get_time()
    
    # Lista para almacenar vuelos que cumplen los criterios
    matching_flights = []
    
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
                # Un retraso positivo indica que salió tarde
                if dep_delay is not None and min_delay <= dep_delay <= max_delay:
                    matching_flights.append(flight)
        
        # Recorrer subárbol derecho
        traverse_tree(node["right"])
    
    # Iniciar recorrido desde la raíz
    traverse_tree(catalog["flights"]["root"])
    
    # Ordenar por retraso ascendente (menor a mayor)
    # Si hay empate en retraso, ordenar por fecha y hora REAL de salida cronológicamente
    matching_flights.sort(key=lambda f: (
        f.get("dep_delay_min") if f.get("dep_delay_min") is not None else float('inf'),
        f.get("dep_dt") if f.get("dep_dt") is not None else datetime.max
    ))
    
    end = get_time()
    exec_time = delta_time(start, end)
    
    total_flights = len(matching_flights)
    
    # Preparar vuelos para mostrar (primeros 5 y últimos 5)
    first_5 = matching_flights[:5] if total_flights > 0 else []
    last_5 = matching_flights[-5:] if total_flights > 5 else []
    
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
    # Ej: [10, 30] -> arr_delay_min en [-30, -10]
    delay_min = -max_early
    delay_max = -min_early

    matching_flights = []

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
                    matching_flights.append(flight)

        traverse(node["right"])

    traverse(catalog["flights"]["root"])

    # Función de llave de ordenamiento definida DENTRO del req_2
    def sort_key(flight):
        """
        Llave de ordenamiento para req_2:
        - Primero por minutos de anticipo (positivo)
        - Luego por fecha/hora real de llegada
        """
        arr_delay = flight.get("arr_delay_min")
        if arr_delay is None:
            anticip = float("inf")
        else:
            if arr_delay < 0:
                anticip = -arr_delay
            else:
                anticip = float("inf")

        arr_dt = flight.get("arr_dt")
        if arr_dt is None:
            arr_dt = datetime.max

        return (anticip, arr_dt)

    # Ordenar por anticipo ascendente (10, 11, 12, ...)
    matching_flights.sort(key=sort_key)

    end = get_time()
    exec_time = delta_time(start, end)
    total_flights = len(matching_flights)

    # Primeros 5 y últimos 5 según el enunciado
    if total_flights > 0:
        first_5 = matching_flights[:5]
    else:
        first_5 = []

    if total_flights > 5:
        last_5 = matching_flights[-5:]
    else:
        last_5 = []

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
    for f in first_5:
        first_5_formatted.append(format_flight(f))

    last_5_formatted = []
    for f in last_5:
        last_5_formatted.append(format_flight(f))

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


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog, date_initial, date_final, hour_initial, hour_final, top_n):
    """
    Retorna el resultado del requerimiento 4
    Identifica las N aerolíneas con mayor número de vuelos en un rango de fechas
    y franja horaria, y obtiene el vuelo con menor duración de cada una.
    
    Args:
        catalog: Catálogo con los datos de vuelos
        date_initial: Fecha inicial del rango (formato YYYY-MM-DD)
        date_final: Fecha final del rango (formato YYYY-MM-DD)
        hour_initial: Hora inicial de la franja (formato HH:MM)
        hour_final: Hora final de la franja (formato HH:MM)
        top_n: Número de aerolíneas a retornar
    
    Returns:
        dict con los resultados del requerimiento
    """
    start = get_time()
    
    # Parsear las fechas y horas de entrada
    fecha_ini = parse_date(date_initial)
    fecha_fin = parse_date(date_final)
    hora_ini = parse_hhmm(hour_initial)
    hora_fin = parse_hhmm(hour_final)
    
    # Diccionario para contar vuelos por aerolínea
    # Key: carrier_code, Value: dict con info de la aerolínea
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
    
    # Convertir diccionario a lista
    airlines_list = list(airlines_data.values())
    
    # Ordenar por número total de vuelos (descendente)
    # Si hay empate, ordenar alfabéticamente por código de aerolínea
    airlines_list.sort(key=lambda a: (-a["total_flights"], a["carrier_code"]))
    
    # Tomar solo las top N aerolíneas
    top_airlines = airlines_list[:top_n]
    
    # Calcular promedios para cada aerolínea
    for airline in top_airlines:
        total_flights = airline["total_flights"]
        if total_flights > 0:
            airline["avg_duration"] = airline["total_duration"] / total_flights
            airline["avg_distance"] = airline["total_distance"] / total_flights
        else:
            airline["avg_duration"] = 0.0
            airline["avg_distance"] = 0.0
    
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
        "total_airlines": len(airlines_list),
        "airlines": formatted_airlines
    }


def req_5(catalog, date_initial, date_final, dest_code, top_n):
    """
    Requerimiento 5:
    Para un aeropuerto de destino y un rango de fechas, identificar las N aerolíneas
    con vuelos más puntuales y, de cada una, obtener el vuelo con la mayor distancia.

    Parámetros:
        catalog: catálogo con los datos de vuelos
        date_initial: fecha inicial (YYYY-MM-DD)
        date_final: fecha final (YYYY-MM-DD)
        dest_code: código del aeropuerto de destino (ej: "JFK")
        top_n: cantidad de aerolíneas a listar
    """
    start = get_time()

    fecha_ini = parse_date(date_initial)
    fecha_fin = parse_date(date_final)

    airlines_data = {}

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

                if carrier not in airlines_data:
                    airlines_data[carrier] = {
                        "carrier_code": carrier,
                        "airline_name": flight.get("airline_name") or "Unknown",
                        "total_flights": 0,
                        "total_duration": 0.0,
                        "total_distance": 0.0,
                        "sum_arr_delay": 0.0,
                        "count_delay": 0,
                        "max_distance_flight": None
                    }

                info = airlines_data[carrier]

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

    airlines_list = []

    for carrier_code in airlines_data:
        info = airlines_data[carrier_code]
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

        airlines_list.append(info)

    # Función de ordenamiento definida DENTRO de req_5
    def sort_key(airline_info):
        avg_delay_val = airline_info["avg_arr_delay_min"]
        if avg_delay_val < 0:
            valor = -avg_delay_val
        else:
            valor = avg_delay_val
        return (valor, airline_info["carrier_code"])

    airlines_list.sort(key=sort_key)

    top_airlines = airlines_list[:top_n]

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
    for info in top_airlines:
        formatted_airlines.append(format_airline(info))

    result = {
        "exec_time_ms": exec_time,
        "date_range": f"{date_initial} a {date_final}",
        "dest_code": dest_code,
        "top_n": top_n,
        "total_airlines": len(airlines_list),
        "airlines": formatted_airlines
    }

    return result

def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


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
