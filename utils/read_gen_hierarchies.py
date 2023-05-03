#!/usr/bin/env python
# coding=utf-8

# Read data and read tree fuctions for ADULTS data
#
# Format:
#   ['age', 'workcalss', 'final_weight', 'education', 'education_num', !marital_status', 'occupation', 'relationship', 'race', 'sex', 'capital_gain', 'capital_loss', 'hours_per_week', 'native_country', 'class']
#   39, State-gov, 77516, Bachelors, 13, Never-married, Adm-clerical, Not-in-family, White, Male, 2174, 0, 40, United-States, <=50K
#   50, Self-emp-not-inc, 83311, Bachelors, 13, Married-civ-spouse, Exec-managerial, Husband, White, Male, 0, 0, 13, United-States, <=50K
#   38, Private, 215646, HS-grad, 9, Divorced, Handlers-cleaners, Not-in-family, White, Male, 0, 0, 40, United-States, <=50K
#
# Attributes 
#   [   'age',  'workcalss',        'final_weight',     'education',    'education_num',    !marital_status',       'occupation',       'relationship',     'race',     'sex',      'capital_gain',     'capital_loss',     'hours_per_week',       'native_country',   'class']
#       39,     State-gov,          77516,              Bachelors,      13,                 Never-married,          Adm-clerical,       Not-in-family,      White,      Male,       2174,               0,                  40`,                    United-States,      <=50K
#       50,     Self-emp-not-inc,   83311,              Bachelors,      13,                 Married-civ-spouse,     Exec-managerial,    Husband,            White,      Male,       0,                  0,                  13,                     United-States,      <=50K
#
# QIDs
#   ['age', 'workcalss', 'education', 'matrital_status', 'race', 'sex', 'native_country']
#
# SA 
#   ['occopation']

from models.gentree import GenTree
from models.numrange import NumRange

import pickle
import pdb

ATTRIBUTE_NAMES = ['age', 'workclass', 'final_weight', 'education',
             'education_num', 'marital_status', 'occupation', 'relationship',
             'race', 'sex', 'capital_gain', 'capital_loss', 'hours_per_week',
             'native_country', 'class']
# 8 attributes are chose as QI attributes
# age and education levels are treated as numeric attributes
# only matrial_status and workclass has well defined generalization hierarchies, other categorical attributes only have 2-level generalization hierarchies.
QID_INDICES = [0, 1, 4, 5, 6, 8, 9, 13]
IS_CAT = [False, True, False, True, True, True, True, True]
SENSITIVE_ATTR_INDEX = -1

__DEBUG = False


# Filter out the QIDs and the SA from the original data file
def read_data() -> list[list[str]]:
    """ Read microda from adult.data, and
            - filter out empty lines
            - filter out lines with missing attribute values
            - remove spaces
            - split lines along commas into string arrays"""
    
    # The number of QIDs
    num_of_qids = len(QID_INDICES)
    # Data with the QID and SA values only
    data: list[list[str]] = []
    unique_value_count_per_attr: list[dict[int|str, int]] = []

    # For all QID attribute, create a dedicated array in the unique_value_count_per_attr array
    for i in range(num_of_qids):
        unique_value_count_per_attr.append(dict())

    # other categorical attributes in intuitive order
    # here, we use the appear number
    data_file = open('data/adult.data', newline=None)

    # Extract the QID attributes and the sensitive attribute into the data variable
    for line in data_file:
        # Remove spaces at the beginning and at the end of the string
        line = line.strip()
        # Remove empty and incomplete lines >> only 30162 records will be kept
        if len(line) == 0 or '?' in line:
            continue

        # Remove spaces from between attribute values, then split the line along commas, creating an array that stores the attribute values from the current line
        line_items = line.replace(' ', '').split(',')

        # Project the original line into one containing only the QID and sensitive attribute values
        projected_line_items: list[str] = []

        for i in range(num_of_qids):
            # Get the index of the current attribute in the original data (nth column it is located in)
            qid_index = QID_INDICES[i]

            # Store how many times each unique value of numerical attributes show up
            if IS_CAT[i] is False:
                try:
                    unique_value_count_per_attr[i][line_items[qid_index]] += 1
                except KeyError:
                    unique_value_count_per_attr[i][line_items[qid_index]] = 1
                    # Copy each attribute value of the line from the temp array to the projected_line_items array
            projected_line_items.append(line_items[qid_index])

        # Add the sensitive attribute value to the projected_line_items array
        projected_line_items.append(line_items[SENSITIVE_ATTR_INDEX])
        data.append(projected_line_items)

    # Write the information gathered about the various numeric attributes values into a new file, through the serialization library named pickle
    # Parsing happens through read_pickle_file
    for i in range(num_of_qids):
        if IS_CAT[i] is False:
            pickle_file = open('data/adult_' + ATTRIBUTE_NAMES[QID_INDICES[i]] + '_static.pickle', 'wb')
            sort_value = list(unique_value_count_per_attr[i].keys())
            sort_value.sort(key=lambda x: int(x))
            pickle.dump((unique_value_count_per_attr[i], sort_value), pickle_file)
            pickle_file.close()

    return data


def read_tree():
    """read tree from data/tree_*.txt, store them in att_tree"""
    
    qid_attribute_names: list[str] = []
    attribute_tree: list[NumRange|dict[str, GenTree]] = []

    # Project the array containing all attribute names into one that contains only the ones that are QIDs
    for qid_index in QID_INDICES:
        qid_attribute_names.append(ATTRIBUTE_NAMES[qid_index])

    for i in range(len(qid_attribute_names)):
        if IS_CAT[i]:
            attribute_tree.append(read_tree_file(qid_attribute_names[i]))
        else:
            attribute_tree.append(read_pickle_file(qid_attribute_names[i]))

    return attribute_tree


def read_pickle_file(attribute_name: str) -> NumRange:
    """ Read pickle file for numeric attributes """

    try:
        pickle_file = open('data/adult_' + attribute_name + '_static.pickle', 'rb')
        (unique_value_count_per_attr, sort_value) = pickle.load(pickle_file)
    except:
        print("Pickle file not exists!!")

    pickle_file.close()

    return NumRange(sort_value, unique_value_count_per_attr)


def read_tree_file(treename: str) -> dict[str, GenTree]:
    """ Read the hierarchy tree from the descriptor file """
    
    attribute_tree = {}    
    attribute_tree['*'] = GenTree('*')

    tree_file = open('data/adult_' + treename + '.txt', newline=None)

    if __DEBUG:
        print("Reading Tree" + treename)

    for line in tree_file:
        # delete \n
        if len(line) <= 1:
            break
                
        line_items = line.strip().split(';')
        # copy line_items
        line_items.reverse()
        
        for i, item in enumerate(line_items):
            isleaf = False

            if i == len(line_items) - 1:
                isleaf = True
                
            # try and except is more efficient than 'in'
            try:
                attribute_tree[item]
            except:
                attribute_tree[item] = GenTree(item, attribute_tree[line_items[i - 1]], isleaf)

    if __DEBUG:
        print("Nodes No. = %d" % attribute_tree['*'].support)

    tree_file.close()

    return attribute_tree
