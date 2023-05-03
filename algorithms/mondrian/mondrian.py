import pdb
import time

from typing import Tuple, List

from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition
from db_connectors.ec_connector import EsConnector


__DEBUG = False
NUM_OF_QIDS_USED = 10
GLOBAL_K = 0
FINAL_PARTITIONS: List[Partition] = []
ATTR_TREES: list[NumRange | dict[str, GenTree]] = []
IS_QID_CATEGORICAL: List[bool] = []
MAX_RANGE_PER_QID = []

ES_CONNECTOR = EsConnector()


def get_normalized_width(partition: Partition, qid_index: int) -> float:    
    """ Return Normalized width of partition """        

    return partition.attr_width_list[qid_index] * 1.0 / MAX_RANGE_PER_QID[qid_index]


def choose_qid(partition: Partition) -> int:
    """ Choose QID with largest normlized width and return its index. """

    max_norm_width = -1
    qid_index = -1

    for i in range(NUM_OF_QIDS_USED):        
        if partition.attr_split_allowed_list[i] == 0:
            continue
        
        normalized_width = get_normalized_width(partition, i)
        
        if normalized_width > max_norm_width:
            max_norm_width = normalized_width
            qid_index = i

    if max_norm_width > 1:
        print("Error: max_norm_width > 1")
        pdb.set_trace()
    if qid_index == -1:
        print("cannot find the max qid_index")
        pdb.set_trace()

    return qid_index


def get_median(partition: Partition, qid_index: int) -> Tuple[str, str, str, str]:
    """ Find the middle of the partition

    Returns
    -------
    (str, str, str, str)
        unique_value_to_split_at: the median
        next_unique_value: the unique value right after the median
        unique_values[0]
        unique_values[-1]
    """

    
    return ES_CONNECTOR.get_median(partition, qid_index)


def split_numerical_value(numeric_value: str, value_to_split_at: int) -> Tuple[str, str] | str:
    """ Split numeric value along value_to_split_at and return sub ranges """

    range_min_and_max = numeric_value.split(',')
    # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
    if len(range_min_and_max) <= 1:
        return range_min_and_max[0], range_min_and_max[0]    
    else:
        min = range_min_and_max[0]
        max = range_min_and_max[1]
        # Create two new partitions using the [mix, value_to_split_at] and [value_to_split_at, max] new ranges
        if min == value_to_split_at:
            l_range = min
        else:
            l_range = min + ',' + value_to_split_at
        if max == value_to_split_at:
            r_range = max
        else:
            r_range = value_to_split_at + ',' + max
            
        return l_range, r_range


def split_numerical_attribute(partition: Partition, qid_index: int) -> list[Partition]:
    """ Split numeric attribute by along the median, creating two new sub-partitions """

    sub_partitions: List[Partition] = []

    (unique_value_to_split_at, next_unique_value, min_unique_value, max_unique_value) = get_median(partition, qid_index)

    # This if-else seems unnecessary already handled in init and then in each iteration through the parts below of this function
    if min_unique_value == max_unique_value:
        partition.attr_gen_list[qid_index] = min_unique_value
    else:
        partition.attr_gen_list[qid_index] = min_unique_value + ',' + max_unique_value

    partition.attr_width_list[qid_index] = max_unique_value - min_unique_value

    if unique_value_to_split_at == '' or unique_value_to_split_at == next_unique_value:        
        return []

    # Copy the current state of the generalization into the new partitions
    l_attr_gen_list = partition.attr_gen_list[:]
    r_attr_gen_list = partition.attr_gen_list[:]

    l_attr_gen_list[qid_index], r_attr_gen_list[qid_index] = split_numerical_value(partition.attr_gen_list[qid_index], unique_value_to_split_at)        

    # The normalized width of all attributes remain the same in the two newly created partitions, except for the one along which we execute the split
    l_attr_width_list = partition.attr_width_list[:]
    r_attr_width_list = partition.attr_width_list[:]

    # The width of the new, "left" partition is composed of the minimum of the original range and the median value    
    l_attr_width_list[qid_index] = unique_value_to_split_at - min_unique_value
    # The width of the new, "right" partition is composed of the next value after the median value we used and the maximal value of the range    
    r_attr_width_list[qid_index] = max_unique_value - next_unique_value

    # TODO: write the query for counting the items in the new subpartitions
    l_count = ES_CONNECTOR.count()
    r_count = ES_CONNECTOR.count()

    sub_partitions.append(Partition(l_count, l_attr_width_list, l_attr_gen_list, NUM_OF_QIDS_USED))
    sub_partitions.append(Partition(r_count, r_attr_width_list, r_attr_gen_list, NUM_OF_QIDS_USED))

    return sub_partitions


def split_categorical_attribute(partition: Partition, qid_index: int) -> list[Partition]:
    """ Split categorical attribute using generalization hierarchy """
    
    node_to_split_at = ATTR_TREES[qid_index][partition.attr_gen_list[qid_index]]
    child_nodes = node_to_split_at.children[:]

    # If the node (has no children, and thus) is a leaf, the partitioning is not possible
    if(len(child_nodes) == 0):
        return []
    
    sub_partitions: List[Partition] = []

    for node in child_nodes:        
        count_of_covered_by_node = ES_CONNECTOR.get_number_of_nodes_covered(partition, qid_index, node.covered_nodes.values())
        
        if count_of_covered_by_node == 0:
            continue

        if count_of_covered_by_node < GLOBAL_K:
            return []
        
        new_attr_width_list = partition.attr_width_list[:]            
        new_attr_gen_list = partition.attr_gen_list[:]

        # For categorical attributes, the width of the attribute equals the number of children
        new_attr_width_list[qid_index] = len(node)
        # The generalized value of the attribute is the node value
        new_attr_gen_list[qid_index] = node.value

        sub_partitions.append(Partition(count_of_covered_by_node, new_attr_width_list, new_attr_gen_list, NUM_OF_QIDS_USED))

    if sum(sub_p.count for sub_p in sub_partitions) != partition.count:
        # TODO: add more details to the exception
        raise Exception("Generalization hierarchy error!")        
    
    return sub_partitions



def split_partition(partition: Partition, qid_index: int):
    """ Split partition and distribute records to different sub-partitions """

    if IS_QID_CATEGORICAL[qid_index] is False:
        return split_numerical_attribute(partition, qid_index)
    else:
        return split_categorical_attribute(partition, qid_index)
    

def check_splitable(partition: Partition):
    """ Check if the partition can be further split while satisfying k-anonymity """

    # If the sum is 0, it means that the allow array only contains 0s, that is no attributes is splittable any more
    if sum(partition.attr_split_allowed_list) == 0:
        return False
    
    return True


def anonymize(partition: Partition):
    """ Main procedure of Half_Partition. Recursively partition groups until not allowable. """
    
    # Close the EC, if not splittable any more
    if check_splitable(partition) is False:
        FINAL_PARTITIONS.append(partition)
        return
    
    qid_index = choose_qid(partition)
    if qid_index == -1:
        print("Error: qid_index=-1")
        pdb.set_trace()

    sub_partitions = split_partition(partition, qid_index)
    if len(sub_partitions) == 0:
        # Close the attribute for this partition, as it cannot be split any more
        partition.attr_split_allowed_list[qid_index] = 0
        anonymize(partition)
    else:
        for sub_p in sub_partitions:
            anonymize(sub_p)


def init(attr_tree: list[NumRange | dict[str, GenTree]], k: int):
    """ Reset all global variables """

    # To change the value of a global variable inside a function, refer to the variable by using the global keyword:
    global GLOBAL_K, FINAL_PARTITIONS, ATTR_TREES, MAX_RANGE_PER_QID, IS_QID_CATEGORICAL
    ATTR_TREES = attr_tree
    GLOBAL_K = k
    FINAL_PARTITIONS = []
    MAX_RANGE_PER_QID = []

    # Based on the received attribute tree, map the attributes into a boolean array that reflects if they are categorical or not
    for tree in attr_tree:
        if isinstance(tree, NumRange):
            IS_QID_CATEGORICAL.append(False)
        else:
            IS_QID_CATEGORICAL.append(True)    
    

# DATA_REQ_EZ: number of qids 
#   - pass to init
#   - pass to whole_partition on initalization
#   - number of records through -> len(data)
def mondrian(attr_tree: list[GenTree | NumRange], k: int):
    """
    Basic Mondrian for k-anonymity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split using the generalization hierarchies.
    The final result is returned in 2-dimensional list.
    """

    init(attr_tree, k)
    
    attr_gen_list = []
    attr_width_list = []

    # TODO: feed in the NUM_OF_QIDS_USED
    for i in range(NUM_OF_QIDS_USED):
        if IS_QID_CATEGORICAL[i] is False:            
            MAX_RANGE_PER_QID.append(ATTR_TREES[i].range)
            attr_width_list.append(ATTR_TREES[i].range)
            attr_gen_list.append(ATTR_TREES[i].value)
        else:
            MAX_RANGE_PER_QID.append(len(ATTR_TREES[i]['*']))
            attr_width_list.append(len(ATTR_TREES[i]['*']))
            attr_gen_list.append('*')

    # TODO: write the query for counting all the documents
    count = ES_CONNECTOR.count()
    whole_partition = Partition(count, attr_width_list, attr_gen_list, NUM_OF_QIDS_USED)
    
    start_time = time.time()
    anonymize(whole_partition)

    rtime = float(time.time() - start_time)
    ncp = 0.0

    for partition in FINAL_PARTITIONS:
        r_ncp = 0.0
        for i in range(NUM_OF_QIDS_USED):
            r_ncp += get_normalized_width(partition, i)

        r_ncp *= len(partition)
        ncp += r_ncp

    # covert to NCP percentage
    ncp /= NUM_OF_QIDS_USED
    ncp /= len(data)
    ncp *= 100

    if len(FINAL_PARTITIONS) != count:
        print("Losing records during anonymization!!")
        pdb.set_trace()

    if __DEBUG:
        print("K=%d" % k)
        print("size of partitions")
        print(len(FINAL_PARTITIONS))
        temp = [len(t) for t in FINAL_PARTITIONS]
        print(sorted(temp))
        print("NCP = %.2f %%" % ncp)

    return (FINAL_PARTITIONS, (ncp, rtime))
