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


def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

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
