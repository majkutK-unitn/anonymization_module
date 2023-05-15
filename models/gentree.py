from __future__ import annotations


class GenTree(object):
    """Class for generalization hierarchies (Taxonomy Tree), tree nodes are stored in the instances.

    Attributes
        value              node value
        level              tree level (top is 0)
        num_of_leaves      number of leaf nodes covered
        parent             ancestor node list
        children           direct successor node list
        covered_nodes      all nodes covered by current node
    """

    def __init__(self, value: str = None, parent: GenTree = None, is_leaf=False):
        self.value = str('')
        self.level = int(0)
        self.num_of_leaves = int(0)
        self.ancestors: list[GenTree] = []
        self.children: list[GenTree] = []
        self.covered_nodes: dict[str, GenTree] = {}

        if value is not None:
            self.value = value
            self.covered_nodes[value] = self

        if parent is not None:
            self.ancestors = parent.ancestors[:]
            # Push to the beginning of the array the direct parent of the node
            self.ancestors.insert(0, parent)
            self.level = parent.level + 1

            # Register oneself as a child of its direct parent
            parent.children.append(self)

            # Register oneself as a covered node of all of its ancestors
            for ancestor in self.ancestors:
                ancestor.covered_nodes[self.value] = self
                if is_leaf:
                    ancestor.num_of_leaves += 1

    def node(self, value: str) -> GenTree|None:
        """ Look for a node with the parameter value."""

        try:
            return self.covered_nodes[value]
        except:
            return None
        
    def values_on_level(self, level: int) -> list[str]:
        values = list(map(lambda filtered_node: filtered_node.value, filter(lambda node: node.level == level, self.covered_nodes.values())))

        if not values:
            raise Exception("Level does not exist in the tree")
        
        return values

    def __len__(self):
        return self.num_of_leaves
