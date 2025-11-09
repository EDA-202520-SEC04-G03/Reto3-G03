import DataStructures.Tree.rbt_node as rbt
from DataStructures.List import single_linked_list as sll

def new_map():
    return {"root": None}

def default_compare(key, element):
    element_key = element["key"] if element is not None else None
    
    if key == element_key:
        return 0
    elif key > element_key:
        return 1
    else: 
        return -1


def size_tree(root):
    if root is None:
        return 0
    return root["size"]

def flip_node_color(node_rbt):
    if node_rbt is None:
        return None
    new_color = rbt.BLACK if rbt.is_red(node_rbt) else rbt.RED
    rbt.change_color(node_rbt, new_color) 
    
    return node_rbt 

def flip_colors(node_rbt):
    
    if node_rbt is None:
        return None
        
    flip_node_color(node_rbt) 
    
    
    if node_rbt["left"] is not None:
        flip_node_color(node_rbt["left"])
        
    
    if node_rbt["right"] is not None:
        flip_node_color(node_rbt["right"])
        
    return node_rbt

def rotate_left(node_rbt):
    
    if node_rbt is None or node_rbt["right"] is None:
        return node_rbt
        
    x = node_rbt
    y = x["right"]
    
    x["right"] = y["left"]
    y["left"] = x
    
    
    y["color"] = x["color"]
    x["color"] = rbt.RED 
    
    
    x["size"] = 1 + size_tree(x["left"]) + size_tree(x["right"])
    y["size"] = 1 + size_tree(y["left"]) + size_tree(y["right"])
    
    return y

def rotate_right(node_rbt):
    
    if node_rbt is None or node_rbt["left"] is None:
        return node_rbt
        
    x = node_rbt
    y = x["left"]
    
    x["left"] = y["right"]
    y["right"] = x
    
    
    y["color"] = x["color"]
    x["color"] = rbt.RED 
    
    x["size"] = 1 + size_tree(x["left"]) + size_tree(x["right"])
    y["size"] = 1 + size_tree(y["left"]) + size_tree(y["right"])
    
    return y

def insert_node(root, key, value):
 
    if root is None:
        return rbt.new_node(key, value) 

    
    cmp = default_compare(key, root)
    
    if cmp == 0:
        
        root["value"] = value
        return root
    elif cmp < 0:
        
        root["left"] = insert_node(root["left"], key, value)
    else: 
        
        root["right"] = insert_node(root["right"], key, value)

    
    
    
    
    if rbt.is_red(root["right"]) and not rbt.is_red(root["left"]):
        root = rotate_left(root)
        
    
    if rbt.is_red(root["left"]) and rbt.is_red(root["left"]["left"]):
        root = rotate_right(root)
        
    
    if rbt.is_red(root["left"]) and rbt.is_red(root["right"]):
        flip_colors(root)
    
    root["size"] = 1 + size_tree(root["left"]) + size_tree(root["right"])

    return root

def put(my_rbt, key, value):
    
    new_root = insert_node(my_rbt["root"], key, value)
    my_rbt["root"] = new_root
    
    rbt.change_color(my_rbt["root"], rbt.BLACK)
    
    return my_rbt

def size(my_rbt):
    return size_tree(my_rbt["root"])


def is_empty(my_rbt):
    return size(my_rbt) == 0


def get(my_rbt, key):
    
    current = my_rbt["root"]
    
    while current is not None:
        cmp = default_compare(key, current)
        
        if cmp == 0:
            return current["value"]
        elif cmp < 0:
            current = current["left"]
        else:
            current = current["right"]
    
    return None

def get_node(root, key):
    
    current = root
    
    while current is not None:
        cmp = default_compare(key, current)
        
        if cmp == 0:
            return current
        elif cmp < 0:
            current = current["left"]
        else:
            current = current["right"]
    
    return None


def contains(my_rbt, key):
    return get(my_rbt, key) is not None

def get_min_node(root):
    current = root
    while current is not None and current["left"] is not None:
        current = current["left"]
    return current

def get_max_node(root):
    current = root
    while current is not None and current["right"] is not None:
        current = current["right"]
    return current

def get_min(my_rbt):
    min_node = get_min_node(my_rbt["root"])
    return min_node["key"] if min_node is not None else None

def get_max(my_rbt):
    max_node = get_max_node(my_rbt["root"])
    return max_node["key"] if max_node is not None else None

def key_set_tree(root, key_list):
    
    if root is None:
        return
    key_set_tree(root["left"], key_list)
    key_list.append(root["key"])
    key_set_tree(root["right"], key_list)


def key_set(my_rbt):
    
    buffer = []
    key_set_tree(my_rbt.get("root"), buffer)

    salida = sll.new_list()
    for k in buffer:
        sll.add_last(salida, k)
    return salida


def value_set_tree(root, value_list):
    
    if root is None:
        return
    value_set_tree(root["left"], value_list)
    value_list.append(root["value"])
    value_set_tree(root["right"], value_list)


def value_set(my_rbt):
    
    buffer = []
    value_set_tree(my_rbt.get("root"), buffer)

    lista = sll.new_list()
    for v in buffer:
        sll.add_last(lista, v)
    return lista

def height_tree(root):
    if root is None:
        return 0
    left_height = height_tree(root["left"])
    right_height = height_tree(root["right"])
    return 1 + max(left_height, right_height)

def height(my_rbt):
    return height_tree(my_rbt["root"])


def keys_range(root, key_initial, key_final, list_key):
    if root is None:
        return

    if key_initial is not None:
        cmp_initial = default_compare(key_initial, root)
        if cmp_initial < 0:
            keys_range(root["left"], key_initial, key_final, list_key)
    else:
        keys_range(root["left"], key_initial, key_final, list_key)

    in_range = True
    if key_initial is not None:
        cmp_initial = default_compare(key_initial, root)
        if cmp_initial > 0:
            in_range = False
    
    if key_final is not None and in_range:
        cmp_final = default_compare(root["key"], {"key": key_final})
        if cmp_final > 0:
            in_range = False

    if in_range:
        sll.add_last(list_key, root["key"])

    if key_final is not None:
        cmp_final = default_compare(root["key"], {"key": key_final})
        if cmp_final < 0:
            keys_range(root["right"], key_initial, key_final, list_key)
    else:
        keys_range(root["right"], key_initial, key_final, list_key)


def keys(my_rbt, key_initial, key_final):
    if key_initial is not None and key_final is not None:
        cmp = default_compare(key_initial, {"key": key_final})
        if cmp > 0:
            return sll.new_list()

    result = sll.new_list()
    keys_range(my_rbt.get("root"), key_initial, key_final, result)
    return result


def values_range(root, key_initial, key_final, value_list):
    if root is None:
        return

    if key_initial is not None:
        cmp_initial = default_compare(key_initial, root)
        if cmp_initial < 0:
            values_range(root["left"], key_initial, key_final, value_list)
    else:
        values_range(root["left"], key_initial, key_final, value_list)

    in_range = True
    if key_initial is not None:
        cmp_initial = default_compare(key_initial, root)
        if cmp_initial > 0:
            in_range = False
    
    if key_final is not None and in_range:
        cmp_final = default_compare(root["key"], {"key": key_final})
        if cmp_final > 0:
            in_range = False

    if in_range:
        sll.add_last(value_list, root["value"])

    if key_final is not None:
        cmp_final = default_compare(root["key"], {"key": key_final})
        if cmp_final < 0:
            values_range(root["right"], key_initial, key_final, value_list)
    else:
        values_range(root["right"], key_initial, key_final, value_list)


def values(my_rbt, key_initial, key_final):
    if key_initial is not None and key_final is not None:
        cmp = default_compare(key_initial, {"key": key_final})
        if cmp > 0:
            return sll.new_list()

    result = sll.new_list()
    values_range(my_rbt.get("root"), key_initial, key_final, result)
    return result
