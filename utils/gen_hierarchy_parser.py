from models.gentree import GenTree
from models.numrange import NumRange


def read_gen_hierarchies_from_text(qid_names: list[str]) -> dict[str, NumRange|GenTree]:
    """ Read genalization hierarchies from gen_hierarchies/*.txt, and store them in qid_dict """

    # { key: value} where key is the attribute name, and the value is the root of the hierarchy tree
    qid_dict: dict[str, GenTree] = {}

    for name in qid_names:
        qid_dict[name] = _create_gen_tree_for_attribute(name)

    return qid_dict


def _create_gen_tree_for_attribute(treename: str) -> dict[str, GenTree]:
    """ Read the hierarchy tree from the descriptor file """

    root = GenTree('*')

    tree_file = open('gen_hierarchies/adult_' + treename + '.txt', newline=None)

    for line in tree_file:
        # delete \n
        if len(line) <= 1:
            break
                
        line_items = line.strip().split(';')
        # copy line_items
        line_items.reverse()
        
        for i, item in enumerate(line_items):
            is_leaf = False

            if i == len(line_items) - 1:
                is_leaf = True                
            
            if root.node(item) == None:
                GenTree(item, root.node(line_items[i - 1]), is_leaf)

    tree_file.close()

    return root


def read_gen_hierarchies_from_json(gen_hierarchies: dict) -> dict[str, GenTree]:
    """ Read the hierarchy tree from the gen_hierarchies attribute of config JSON file """

    qid_dict: dict[str, GenTree] = {}

    for hier_name, value in gen_hierarchies.items():                
        root = _get_child_nodes(value["tree"], None)
        qid_dict[hier_name] = root

    return qid_dict


def _get_child_nodes(node, tree_parent: GenTree):
    is_leaf = not bool(node["children"])        
    tree_node = GenTree(node["value"], tree_parent, is_leaf)

    if not is_leaf:
        for child in node["children"]:
            _get_child_nodes(child, tree_node)

    return tree_node

