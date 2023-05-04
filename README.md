POC for the thesis project

# Adults dataset

## Format

['age', 'workcalss', 'final_weight', 'education', 'education_num', !marital_status', 'occupation', 'relationship', 'race', 'sex', 'capital_gain', 'capital_loss', 'hours_per_week', 'native_country', 'class']  
39, State-gov, 77516, Bachelors, 13, Never-married, Adm-clerical, Not-in-family, White, Male, 2174, 0, 40, United-States, <=50K  
50, Self-emp-not-inc, 83311, Bachelors, 13, Married-civ-spouse, Exec-managerial, Husband, White, Male, 0, 0, 13, United-States, <=50K  
38, Private, 215646, HS-grad, 9, Divorced, Handlers-cleaners, Not-in-family, White, Male, 0, 0, 40, United-States, <=50K

## QIDs
['age', 'workcalss', 'education', 'matrital_status', 'race', 'sex', 'native_country']

- age and education levels are treated as numeric attributes
- matrial_status and workclass has well defined generalization hierarchies, other categorical attributes only have 2-level generalization hierarchies

## SA 
['occopation']