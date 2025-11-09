# priority_queue.py

from DataStructures.List import array_list as al
from DataStructures.Priority_queue import pq_entry as pqe

def default_compare_lower_value(father_node, child_node):
    if pqe.get_priority(father_node) <= pqe.get_priority(child_node):
        return True
    return False

def default_compare_higher_value(father_node, child_node):
    if pqe.get_priority(father_node) >= pqe.get_priority(child_node):
        return True
    return False

def new_heap(is_min_pq=True):
    queue = {
        "elements": al.new_list(),
        "size": 0,
        "cmp_function": default_compare_lower_value if is_min_pq else default_compare_higher_value
    }
    al.add_last(queue["elements"], None)
    return queue

def size(my_heap):
    return my_heap["size"]

def is_empty(my_heap):
    return my_heap["size"] == 0

def swim(my_heap, pos):
    elements = my_heap["elements"]
    cmp_function = my_heap["cmp_function"]

    while pos > 1:
        parent_pos = pos // 2
        
        child_node = al.get_element(elements, pos)
        father_node = al.get_element(elements, parent_pos)
        
        if not cmp_function(father_node, child_node):
            al.exchange(elements, pos, parent_pos)
            pos = parent_pos
        else:
            pos = 0 

    return my_heap

def priority(my_heap, parent, child):
    return my_heap["cmp_function"](parent, child)

def exchange(my_heap, pos1, pos2):
    al.exchange(my_heap["elements"], pos1, pos2)
    return my_heap

def sink(my_heap, pos):
    elements = my_heap["elements"]
    heap_size = size(my_heap)
    continuar = (2 * pos) <= heap_size
    
    while continuar:
        child = 2 * pos         
        
        if child < heap_size and not priority(my_heap, al.get_element(elements, child), al.get_element(elements, child + 1)):
            child += 1  
        
        if not priority(my_heap,al.get_element(elements, pos), al.get_element(elements, child)):
            exchange(my_heap, pos, child)
            pos = child
            continuar = (2 * pos) <= heap_size
        else:
            continuar = False
    
    return my_heap

def remove(my_heap):
    if is_empty(my_heap):
        return None
        
    elements = my_heap["elements"]
    root = al.get_element(elements, 1)
    root_value = pqe.get_value(root)
    
    last = al.get_element(elements, my_heap["size"])
    al.change_info(elements, 1, last)
    
    al.remove_last(elements)
    my_heap["size"] -= 1
    
    if not is_empty(my_heap):
        sink(my_heap, 1)
        
    return root_value

def get_first_priority(my_heap):
    if is_empty(my_heap):
        return None
    
    first = al.get_element(my_heap["elements"], 1)
    return pqe.get_value(first)

def insert(my_heap, priority, value):
    new_entry = pqe.new_pq_entry(priority, value)
    al.add_last(my_heap["elements"], new_entry)
    my_heap["size"] += 1
    new_pos = my_heap["size"]
    my_heap = swim(my_heap, new_pos)
    return my_heap

def is_present_value(my_heap, value):
    elements = my_heap["elements"]
    heap_size = size(my_heap)

    for pos in range(heap_size):
        entry = al.get_element(elements, pos)
        if pqe.get_value(entry) == value:
            return pos

    return -1