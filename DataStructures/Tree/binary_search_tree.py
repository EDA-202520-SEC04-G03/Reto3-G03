import DataStructures.Tree.bst_node as bst
import DataStructures.List.single_linked_list as ll


def new_map():
    return {"root": None}

def insert_node(root, key, value):
    if root is None:
        return bst.new_node(key, value)

    if key < root["key"]:
        root["left"] = insert_node(root["left"], key, value)
    elif key > root["key"]:
        root["right"] = insert_node(root["right"], key, value)
    else:
        root["value"] = value
    return root

def put(my_bst, key, value):
    my_bst["root"] = insert_node(my_bst["root"], key, value)
    return my_bst

def get_node(root, key):
    if root is None:
        return None
    if key < root["key"]:
        return get_node(root["left"], key)
    elif key > root["key"]:
        return get_node(root["right"], key)
    else:
        return root

def get(my_bst, key):
    node = get_node(my_bst["root"], key)
    return None if node is None else node["value"]

def size_tree(root):
    if root is None:
        return 0
    return 1 + size_tree(root["left"]) + size_tree(root["right"])

def size(my_bst):
    return size_tree(my_bst["root"])

def is_empty(my_bst):
    return my_bst["root"] is None   

def contains(my_bst, key):
    return get_node(my_bst["root"], key) is not None

def get_min_node(my_bst):
    current = my_bst["root"]
    if current is None:
        return None
    while current["left"] is not None:
        current = current["left"]
    return current


def get_min(my_bst):
    node = get_min_node(my_bst)
    return None if node is None else node["key"]


def get_max_node(my_bst):
    current = my_bst["root"]
    if current is None:
        return None
    while current["right"] is not None:
        current = current["right"]
    return current


def get_max(my_bst):
    node = get_max_node(my_bst)
    return None if node is None else node["key"]


def height_tree(root):
    if root is None:
        return 0
    left_height = height_tree(root["left"])
    right_height = height_tree(root["right"])
    return 1 + (left_height if left_height > right_height else right_height)


def height(my_bst):
    return height_tree(my_bst["root"])

def delete_min_tree(root):
    if root is None:
        return None
    if root["left"] is None:
        return root["right"]
    root["left"] = delete_min_tree(root["left"])
    return root


def delete_min(my_bst):
    my_bst["root"] = delete_min_tree(my_bst["root"])
    return my_bst


def delete_max_tree(root):
    if root is None:
        return None
    if root["right"] is None:
        return root["left"]
    root["right"] = delete_max_tree(root["right"])
    return root


def delete_max(my_bst):
    my_bst["root"] = delete_max_tree(my_bst["root"])
    return my_bst

def key_set_tree(root, key_list):
    
    if root is not None:
        
        key_set_tree(root["left"], key_list)
         
        ll.add_last(key_list, root["key"])

        key_set_tree(root["right"], key_list)

def key_set(my_bst):
    
    key_list = ll.new_list()

    root = my_bst["root"]
    
    key_set_tree(root, key_list)
    
    return key_list

def value_set_tree(root, value_list):
    
    if root is not None:
    
        value_set_tree(root["left"], value_list)

        ll.add_last(value_list, root["value"])

        value_set_tree(root["right"], value_list)

def value_set(my_bst):
    
    value_list = ll.new_list()

    root = my_bst["root"]
    
    value_set_tree(root, value_list)
    
    return value_list   

def keys_range(root, key_initial, key_final, list_key):
    if root is None:
        return
    if key_initial < root["key"]:
        keys_range(root["left"], key_initial, key_final, list_key)
    if key_initial <= root["key"] <= key_final:
        ll.add_last(list_key, root["key"])
    if key_final > root["key"]:
        keys_range(root["right"], key_initial, key_final, list_key)

def keys(my_bst, key_initial, key_final):
    list_key = ll.new_list()
    keys_range(my_bst["root"], key_initial, key_final, list_key)
    return list_key

def values_range(root, key_lo, key_hi, list_values):
    if root is None:
        return
    if key_lo < root["key"]:
        values_range(root["left"], key_lo, key_hi, list_values)
    if key_lo <= root["key"] <= key_hi:
        ll.add_last(list_values, root["value"])
    if key_hi > root["key"]:
        values_range(root["right"], key_lo, key_hi, list_values)

def values(my_bst, key_initial, key_final):
    list_values = ll.new_list()
    values_range(my_bst["root"], key_initial, key_final, list_values)
    return list_values