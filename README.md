POC for the thesis project

# Adults dataset

## Attributes

    - age
    - workclass
    - final_weight
	- education
	- education_num, marital_status
	- occupation
	- relationship
	- race
	- sex
	- capital_gain
	- capital_loss
	- hours_per_week
	- native_country
	- class

## Quasi-identifiers (QIDs)

    - [numerical] age
	- [hierarchical] workclass
	- [numerical] education_level
	- [hierarchical] matrital_status
	- [categorical] race
	- [categorical] sex
	- [categorical] native_country


Note: despite being categorical attributes, race, sex and native_country are treated as hierarchical ones through a one-level generalization hierarchy.

## Sensitive attribute
    - class


## Some records

| age   | workclass | final_weight  | education | education_num | marital_status    | occupation    | relationship  | race  | sex   | capital_gain  | capital_loss  | hours_per_week    | native_country    | class |
|  ---  |  ---      |  ---          |  ---      |  ---          | ---               |  ---          |  ---          |  ---  |  ---  |  ---          |  ---          |  ---              | ---               |  ---  |
| 39    | State-gov | 77516         | Bachelors | 13            | Never-married     | Adm-clerical  | Not-in-family | White | Male  | 2174          | 0             | 40                | United-States     | <=50K  |
| 50    | Self-emp-not-inc | 83311  | Bachelors | 13            | Married-civ-spouse | Exec-managerial | Husband    | White | Male  | 0             | 0             | 13                | United-States     | <=50K  | 
| 38    | Private   | 215646        | HS-grad   | 9             | Divorced          | Handlers-cleaners | Not-in-family | White | Male | 0          | 0             | 40                | United-States     | <=50K| 



# Algorithms

The Mondrian implementation is based on the [Basic Mondiran repository of Qiyuan Gong](https://github.com/qiyuangong/Basic_Mondrian).
