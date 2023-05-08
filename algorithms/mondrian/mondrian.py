import time

from typing import Tuple

from models.attribute import Attribute
from models.gentree import GenTree
from models.numrange import NumRange
from models.partition import Partition

from db_connectors.ec_connector import EsConnector


__DEBUG = False

GLOBAL_K = 0
FINAL_PARTITIONS: list[Partition] = []

ES_CONNECTOR = EsConnector()


def get_normalized_width(partition: Partition, qid_name: str) -> float:    
    """ Return Normalized width of partition """        

    return partition.attributes[qid_name].width * 1.0 / len(Partition.attr_dict[qid_name])


def choose_qid_name(partition: Partition) -> str:
    """ Choose QID with largest normlized width and return its index. """

    max_norm_width = -1
    qid_name: str = None

    for attr_name in partition.attributes.keys():
        if partition.attributes[attr_name].split_allowed:
            normalized_width = get_normalized_width(partition, attr_name)
        
            if normalized_width > max_norm_width:
                max_norm_width = normalized_width
                qid_name = attr_name

    if max_norm_width > 1:        
        raise Exception("Max normalized width is greater than 1")

    return qid_name


def split_numerical_value(numeric_value: str, value_to_split_at: int) -> Tuple[str, str]:
    """ Split numeric value along value_to_split_at and return sub ranges """

    range_min_and_max = numeric_value.split(',')
    # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
    if len(range_min_and_max) <= 1:
        return range_min_and_max[0], range_min_and_max[0]    
    else:
        min_value = int(range_min_and_max[0])
        max_value = int(range_min_and_max[1])
        # Create two new partitions using the [mix, value_to_split_at] and [value_to_split_at, max] new ranges
        if min_value == value_to_split_at:
            l_range = str(min_value)
        else:
            l_range = f"{min_value},{value_to_split_at}"
        if max_value == value_to_split_at:
            r_range = str(max_value)
        else:
            r_range = f"{value_to_split_at},{max_value}"
            
        return l_range, r_range


def split_numerical_attribute(partition: Partition, qid_name: str) -> list[Partition]:
    """ Split numeric attribute by along the median, creating two new sub-partitions """

    sub_partitions: list[Partition] = []
    
    (median, next_unique_value) = ES_CONNECTOR.get_attribute_median_and_next_unique_value(partition.attributes, qid_name)
    (min_value, max_value) = ES_CONNECTOR.get_attribute_min_max(qid_name, partition.attributes)

    # This if-else seems unnecessary already handled in init and then in each iteration through the parts below of this function
    if min_value == max_value:
        partition.attributes[qid_name].gen_value = str(min_value)
    else:
        partition.attributes[qid_name].gen_value = f"{min_value},{max_value}"

    partition.attributes[qid_name].width = max_value - min_value

    if median is None or next_unique_value is None:
        return []

    l_attributes = partition.attributes.copy()
    r_attributes = partition.attributes.copy()

    (l_gen_value, r_gen_value) = split_numerical_value(partition.attributes[qid_name].gen_value, median)
    
    l_width = median - min_value    
    r_width = max_value - next_unique_value

    l_attributes[qid_name] = Attribute(l_width, l_gen_value)
    r_attributes[qid_name] = Attribute(r_width, r_gen_value)

    l_count = ES_CONNECTOR.get_document_count(l_attributes)
    r_count = ES_CONNECTOR.get_document_count(r_attributes)

    if l_count < GLOBAL_K or r_count < GLOBAL_K:
        return []

    sub_partitions.append(Partition(l_count, l_attributes))
    sub_partitions.append(Partition(r_count, r_attributes))

    return sub_partitions


def split_categorical_attribute(partition: Partition, qid_name: str) -> list[Partition]:
    """ Split categorical attribute using generalization hierarchy """

    node_to_split_at: GenTree = Partition.attr_dict[qid_name].node(partition.attributes[qid_name].gen_value)

    # If the node (has no children, and thus) is a leaf, the partitioning is not possible
    if not len(node_to_split_at.children):
        return []
    
    sub_partitions: list[Partition] = []

    for child in node_to_split_at.children:
        generalized_attrs = partition.attributes.copy()
        generalized_attrs[qid_name] = Attribute(len(child), child.value)

        count_covered_by_child = ES_CONNECTOR.get_document_count(generalized_attrs)
        
        if count_covered_by_child == 0:
            continue

        if count_covered_by_child < GLOBAL_K:
            return []
        
        sub_partitions.append(Partition(count_covered_by_child, generalized_attrs))

    if sum(sub_p.count for sub_p in sub_partitions) != partition.count:
        # TODO: uncover root cause of anomaly
        #   - splitting along some categorical attributes seems to lose data
        #   - actually, the original partition count seems to be flawed
        #       - might be caused by overlapping ECS
        #       - the query, generated at this point from the original partition attributes, gives a different count
        #           - with this count, the subpartition counts are consistent
        #       BUT at the end of the algorithm there is indeed more than 5000 documents lost if summing up the final partition counts        
        #           - might be caused by the currently flawed way of splitting numerical attributes
        #           - might be caused by some anomaly in the original assignment of the count when creating the partition
        raise Exception("The number of items in the subpartitions is not equal to that of the original partition")        
    
    return sub_partitions


def split_partition(partition: Partition, qid_name: str):
    """ Split partition and distribute records to different sub-partitions """

    if isinstance(Partition.attr_dict[qid_name], NumRange):
        return split_numerical_attribute(partition, qid_name)
    else:
        return split_categorical_attribute(partition, qid_name)
    

def check_splitable(partition: Partition):
    """ Check if the partition can be further split while satisfying k-anonymity """

    # The sum of all the boolean values is True, if any of the attributes is splittable
    if sum(map(lambda part: part.split_allowed, partition.attributes.values())):
        return True
        
    return False
    


def anonymize(partition: Partition):
    """ Main procedure of Half_Partition. Recursively partition groups until not allowable. """
    
    # Close the EC, if not splittable any more
    if check_splitable(partition) is False:
        FINAL_PARTITIONS.append(partition)
        return
    
    qid_name = choose_qid_name(partition)

    if qid_name == None:
        raise Exception("No QID was chosen in the choose_qid_name call")        

    sub_partitions = split_partition(partition, qid_name)
    if len(sub_partitions) == 0:
        # Close the attribute for this partition, as it cannot be split any more
        partition.attributes[qid_name].split_allowed = False
        anonymize(partition)
    else:
        for sub_p in sub_partitions:
            anonymize(sub_p)


def init(gen_hiers: dict[str, GenTree], qid_names: list[str], k: int):
    """ Reset all global variables """

    # To change the value of a global variable inside a function, refer to the variable by using the global keyword:
    global GLOBAL_K, FINAL_PARTITIONS    
    GLOBAL_K = k
    FINAL_PARTITIONS = []

    attributes: dict[str, Attribute] = {}
    gen_hiers_and_num_ranges: dict[str, NumRange|GenTree] = gen_hiers
    
    for attr_name in qid_names:
        root_node_or_num_range: NumRange | GenTree

        if attr_name in gen_hiers:
            root_node_or_num_range = gen_hiers[attr_name]            
        else:            
            (min, max) = ES_CONNECTOR.get_attribute_min_max(attr_name)
            root_node_or_num_range = NumRange(min, max)
            gen_hiers_and_num_ranges[attr_name] = root_node_or_num_range
        
        attributes[attr_name] = Attribute(len(root_node_or_num_range), root_node_or_num_range.value)
    
    count = ES_CONNECTOR.get_document_count()
    whole_partition = Partition(count, attributes)

    Partition.attr_dict = gen_hiers_and_num_ranges
    
    return whole_partition
    

def mondrian(gen_hiers: dict[str, GenTree], qid_names: list[str], k: int):
    """
    Basic Mondrian for k-anonymity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split using the generalization hierarchies.
    The final result is returned in 2-dimensional list.
    """

    whole_partition = init(gen_hiers, qid_names, k)   
    
    start_time = time.time()

    anonymize(whole_partition)


    # Post-processing
    rtime = float(time.time() - start_time)
    ncp = 0.0

    for partition in FINAL_PARTITIONS:
        r_ncp = 0.0
        for attr_name in qid_names:
            r_ncp += get_normalized_width(partition, attr_name)

        r_ncp *= partition.count
        ncp += r_ncp

    # covert to NCP percentage
    ncp /= len(qid_names)
    ncp /= whole_partition.count
    ncp *= 100

    if sum(map(lambda partition: partition.count, FINAL_PARTITIONS)) != whole_partition.count:        
        raise Exception("Losing records during anonymization")

    if __DEBUG:
        print("K=%d" % k)
        print("size of partitions")
        print(len(FINAL_PARTITIONS))
        temp = [p.count for p in FINAL_PARTITIONS]
        print(sorted(temp))
        print("NCP = %.2f %%" % ncp)

    return (FINAL_PARTITIONS, (ncp, rtime))
