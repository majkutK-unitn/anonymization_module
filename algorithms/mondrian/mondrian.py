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
RESULT: List[Partition] = []
ATT_TREES: List[GenTree | NumRange] = []
IS_QID_CATEGORICAL: List[bool] = []
QI_RANGE = []

ES_CONNECTOR = EsConnector()


def get_normalized_width(partition: Partition, qid_index: int) -> float:    
    """ Return Normalized width of partition """        

    return partition.attribute_width_list[qid_index] * 1.0 / QI_RANGE[qid_index]


def choose_qid(partition: Partition) -> int:
    """ Choose QID with largest normlized width and return its index. """

    max_norm_width = -1
    qid_index = -1

    for i in range(NUM_OF_QIDS_USED):        
        if partition.attribute_split_allowed_list[i] == 0:
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
        partition.attribute_generalization_list[qid_index] = min_unique_value
    else:
        partition.attribute_generalization_list[qid_index] = min_unique_value + ',' + max_unique_value

    partition.attribute_width_list[qid_index] = max_unique_value - min_unique_value

    if unique_value_to_split_at == '' or unique_value_to_split_at == next_unique_value:        
        return []

    # Copy the current state of the generalization into the new partitions
    l_attribute_generalization_list = partition.attribute_generalization_list[:]
    r_attribute_generalization_list = partition.attribute_generalization_list[:]

    l_attribute_generalization_list[qid_index], r_attribute_generalization_list[qid_index] = split_numerical_value(partition.attribute_generalization_list[qid_index], unique_value_to_split_at)        

    # The normalized width of all attributes remain the same in the two newly created partitions, except for the one along which we execute the split
    l_attribute_width_list = partition.attribute_width_list[:]
    r_attribute_width_list = partition.attribute_width_list[:]

    # The width of the new, "left" partition is composed of the minimum of the original range and the median value    
    l_attribute_width_list[qid_index] = unique_value_to_split_at - min_unique_value
    # The width of the new, "right" partition is composed of the next value after the median value we used and the maximal value of the range    
    r_attribute_width_list[qid_index] = max_unique_value - next_unique_value

    # TODO: write the query for counting the items in the new subpartitions
    l_count = ES_CONNECTOR.count()
    r_count = ES_CONNECTOR.count()

    sub_partitions.append(Partition(l_count, l_attribute_width_list, l_attribute_generalization_list, NUM_OF_QIDS_USED))
    sub_partitions.append(Partition(r_count, r_attribute_width_list, r_attribute_generalization_list, NUM_OF_QIDS_USED))

    return sub_partitions

# DATA_REQ
#   - direct processing through "for record in partition.members"
#       - see if the child nodes in the hierarchy cover all value AND if the all new partitions are greater than k
#------------------------------------------------------------------------
#   >> execute the queries with the refined attribute values
#       - if for all new EC ec, refined along the current attribute, |ec| > k OR |EC| = 0: the attribute can be split
#       - else the splitting along this attribute is not possble any more 
def split_categorical_attribute(partition: Partition, qid_index: int) -> list[Partition]:
    """ Split categorical attribute using generalization hierarchy """

    sub_partitions: List[Partition] = []
    
    node_to_split_at = ATT_TREES[qid_index][partition.attribute_generalization_list[qid_index]]
    child_nodes = node_to_split_at.children[:]    

    sub_groups = []
    for i in range(len(child_nodes)):
        sub_groups.append([])

    # If the node (has no children, and thus) is a leaf, the partitioning is not possible >> []
    if len(sub_groups) == 0:        
        return []
    
    # DATA_REQ >> MAP_TO_QUERY
    for record in partition.members:
        qid_value = record[qid_index]
        for i, node in enumerate(child_nodes):
            try:
                node.cover[qid_value]
                # Store the records in the sub_groups array under the index that corresponds to the index of the child of the current node
                sub_groups[i].append(record)
                break
            except KeyError:
                continue
        # If for one of the records of the partition we do not find a QID value from the child nodes of the current node, it cannot be generalized
        # In this case, the try block never reaches the break, thus the for runs all the way to the end and the execution reaches this else branch
        else:
            print("Generalization hierarchy error!")

    flag = True
    for sub_group in sub_groups:
        if len(sub_group) == 0:
            continue
        # If one child covers less than k elements, the split is invalid
        if len(sub_group) < GLOBAL_K:
            flag = False
            break

    if flag:
        for i, sub_group in enumerate(sub_groups):
            if len(sub_group) == 0:
                continue

            new_attribute_width_list = partition.attribute_width_list[:]            
            new_attribute_generalization_list = partition.attribute_generalization_list[:]

            # For categorical attributes, the width of the attribute equals the number of children
            new_attribute_width_list[qid_index] = len(child_nodes[i])
            # The generalized value of the attribute is the node value
            new_attribute_generalization_list[qid_index] = child_nodes[i].value

            sub_partitions.append(Partition(sub_group, new_attribute_width_list, new_attribute_generalization_list, NUM_OF_QIDS_USED))

    return sub_partitions



def split_partition(partition: Partition, qid_index: int):
    """ Split partition and distribute records to different sub-partitions """    
    if IS_QID_CATEGORICAL[qid_index] is False:
        return split_numerical_attribute(partition, qid_index)
    else:
        return split_categorical_attribute(partition, qid_index)


def anonymize(partition: Partition):
    """ Main procedure of Half_Partition. Recursively partition groups until not allowable.
    """
    # print(len(partition)
    # print(partition.attribute_split_allowed_list
    # pdb.set_trace()

    # Close the EC, if not splittable any more
    if check_splitable(partition) is False:
        RESULT.append(partition)
        return
    
    qid_index = choose_qid(partition)
    if qid_index == -1:
        print("Error: qid_index=-1")
        pdb.set_trace()

    sub_partitions = split_partition(partition, qid_index)
    if len(sub_partitions) == 0:
        # Close the attribute for this partition, as it cannot be split any more
        partition.attribute_split_allowed_list[qid_index] = 0
        anonymize(partition)
    else:
        for sub_p in sub_partitions:
            anonymize(sub_p)


def check_splitable(partition: Partition):
    """ Check if the partition can be further split while satisfying k-anonymity """

    # If the sum is 0, it means that the allow array only contains 0s, that is no attributes is splittable any more
    if sum(partition.attribute_split_allowed_list) == 0:
        return False
    return True

# DATA_REQ_EZ: number of qids 
#   - number of qids through len(data[0]) - 1
def init(att_trees: List[GenTree | NumRange], data, k: int, QI_num=-1):
    """ Reset all global variables """

    # To change the value of a global variable inside a function, refer to the variable by using the global keyword:
    global GLOBAL_K, RESULT, NUM_OF_QIDS_USED, ATT_TREES, QI_RANGE, IS_QID_CATEGORICAL
    ATT_TREES = att_trees

    # Based on the received attribute tree, map the attributes into a boolean array that reflects if they are categorical or not
    for tree in att_trees:
        if isinstance(tree, NumRange):
            IS_QID_CATEGORICAL.append(False)
        else:
            IS_QID_CATEGORICAL.append(True)

    # Use all QIDs in this case
    if QI_num <= 0:
        # We do not need the SA that is appended to each line as the last value
        NUM_OF_QIDS_USED = len(data[0]) - 1
    else:
        # Use only the desired number of QIDs
        NUM_OF_QIDS_USED = QI_num

    GLOBAL_K = k
    RESULT = []
    QI_RANGE = []


# DATA_REQ_EZ: number of qids 
#   - pass to init
#   - pass to whole_partition on initalization
#   - number of records through -> len(data)
def mondrian(att_trees: list[GenTree | NumRange], data: list[list[str]], k: int, QI_num=-1):
    """
    basic Mondrian for k-anonymity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split on GH.
    The final result is returned in 2-dimensional list.
    """
    init(att_trees, data, k, QI_num)
    result = []
    attribute_generalization_list = []
    attribute_width_list = []

    for i in range(NUM_OF_QIDS_USED):
        if IS_QID_CATEGORICAL[i] is False:
            QI_RANGE.append(ATT_TREES[i].range)
            attribute_width_list.append((0, len(ATT_TREES[i].sort_value) - 1))
            attribute_generalization_list.append(ATT_TREES[i].value)
        else:
            QI_RANGE.append(len(ATT_TREES[i]['*']))
            attribute_width_list.append(len(ATT_TREES[i]['*']))
            attribute_generalization_list.append('*')

    whole_partition = Partition(data, attribute_width_list, attribute_generalization_list, NUM_OF_QIDS_USED)
    
    start_time = time.time()
    anonymize(whole_partition)

    rtime = float(time.time() - start_time)
    ncp = 0.0
    for partition in RESULT:
        r_ncp = 0.0
        for i in range(NUM_OF_QIDS_USED):
            r_ncp += get_normalized_width(partition, i)
        temp = partition.attribute_generalization_list
        for i in range(len(partition)):
            result.append(temp + [partition.members[i][-1]])
        r_ncp *= len(partition)
        ncp += r_ncp
    # covert to NCP percentage
    ncp /= NUM_OF_QIDS_USED
    ncp /= len(data)
    ncp *= 100
    if len(result) != len(data):
        print("Losing records during anonymization!!")
        pdb.set_trace()
    if __DEBUG:
        print("K=%d" % k)
        print("size of partitions")
        print(len(RESULT))
        temp = [len(t) for t in RESULT]
        print(sorted(temp))
        print("NCP = %.2f %%" % ncp)
    return (result, (ncp, rtime))
