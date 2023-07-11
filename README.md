# POC for an anonymization module

# Running the project

1. Set the environment variables for the database backend

	- Elasticsearch
		```
		(set ES_HOST=<<database_host>>)
		(set API_KEY_BASE64=<<api_key>>)
		(set ROOT_CA_PATH=<<path_to_root_ca>>)		
		(set INDEX_NAME=adults)
		```


	- MySQL
		```
		(set MYSQL_HOST=<<database_host>>)
		(set MYSQL_USER=anon_module)
		(set MYSQL_PASSWORD=<<database_password>>)
		(set MYSQL_DATABASE=<<database_name>>)
		(set MYSQL_TABLE_NAME=adults)
		```

2. Through the command line arguments, specify
	- which algorithm to run
	- which database to connect to
	- which config file to use

	```
	python main.py --algorithm [mondrian/datafly] --backend [es/mysql] --config [adults.json/kibana_data_logs.json]
	```

# Datasets

The project currently contains configuration files for two datasets:
- [adults](http://archive.ics.uci.edu/ml/datasets/adult)
- [kibana_sample_logs](https://www.elastic.co/guide/en/kibana/8.7/get-started.html#gs-get-data-into-kibana)

## Adults

### Attributes

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

#### Sensitive attribute: class	

#### Quasi-identifiers (QIDs) used

| name   | type |	
|--------|------|
| age   | numerical |
| education_level   | numerical |
| workclass   | hierarchical |
| occupation   | hierarchical |
| matrital_status   | hierarchical |
| race   | hierarchical |
| sex   | hierarchical |
| native_country   | hierarchical |

*Note*: despite being categorical attributes, race, sex and native_country are treated as hierarchical ones through a one-level generalization hierarchy.




### Preview of the data

| age   | workclass | final_weight  | education | education_num | marital_status    | occupation    | relationship  | race  | sex   | capital_gain  | capital_loss  | hours_per_week    | native_country    | class |
|  ---  |  ---      |  ---          |  ---      |  ---          | ---               |  ---          |  ---          |  ---  |  ---  |  ---          |  ---          |  ---              | ---               |  ---  |
| 39    | State-gov | 77516         | Bachelors | 13            | Never-married     | Adm-clerical  | Not-in-family | White | Male  | 2174          | 0             | 40                | United-States     | <=50K  |
| 50    | Self-emp-not-inc | 83311  | Bachelors | 13            | Married-civ-spouse | Exec-managerial | Husband    | White | Male  | 0             | 0             | 13                | United-States     | <=50K  | 
| 38    | Private   | 215646        | HS-grad   | 9             | Divorced          | Handlers-cleaners | Not-in-family | White | Male | 0          | 0             | 40                | United-States     | <=50K  |

### Ingestion into Elasticsearch via Logstash

1. Clean the data
	- remove the attribute name if they are included as the first line of the file
	- remove empty lines
	- remove data with empty fields
	- remove spaces

	```
	cat <<path_to_adults_data>> | grep -v -e "^$" | grep -v "?" | sed 's/ //g' > /tmp/adults.data
	```
        
2. Create an adults index with mapping

	```
	PUT /adults
	{
		"mappings": {
			"properties": { 
				"age": { "type": "integer" },
				"education_num": { "type": "integer" },
				"capital_gain": { "type": "keyword" },
				"capital_loss": { "type": "keyword" },
				"class": { "type": "keyword" },
				"education": { "type": "keyword" },
				"final_weight": { "type": "keyword" }, 
				"host": { "type": "keyword" }, 
				"hours_per_week": { "type": "keyword" }, 
				"marital_status": { "type": "keyword" }, 
				"message": { "type": "keyword" }, 
				"native_country": { "type": "keyword" }, 
				"occupation": { "type": "keyword" }, 
				"path": { "type": "keyword" }, 
				"race": { "type": "keyword" }, 
				"relationship": { "type": "keyword" }, 
				"sex": { "type": "keyword" }, 
				"workclass": { "type": "keyword" }
			}
		}
	}
	```

3. Make sure that the logstash role has the rights to create an index with the specified name ("adults", in this case) 	

4. Ingest the data via Logstash

	```
	input {
		file {
			path => ["/tmp/adults.data"]
			# To force Logstash to reparse the file https://stackoverflow.com/questions/19546900/how-to-force-logstash-to-reparse-a-file
			# start_position => "beginning"
			# sincedb_path => "/dev/null"
		}
	}

	filter {
		csv {                        
			columns => ["age","workclass","final_weight","education","education_num","marital_status","occupation","relationship","race","sex","capital_gain","capital_loss","hours_per_week","native_country","class"]
		}
	}

	output {
		elasticsearch {
			hosts => ["<<Elasticsearch host>>"]
			index => "adults"
			codec => "plain"
			manage_template => false
			ssl => true			
			cacert => "<<path_to_ca_cert>>"
			ssl_certificate_verification => true
			keystore => "<<path_to_keystore>>"
			keystore_password => ""
		}
	}
	```


### Ingestion into MySQL

1. Clean the data
	- remove empty lines
	- remove data with empty fields
	- remove spaces

	```
	cat <<path_to_adults_data>> | grep -v -e "^$" | grep -v "?" | sed 's/ //g' > /tmp/adults.data
	```

2. Create the adults table

	```
	CREATE TABLE adults (
		age INT,
		workclass VARCHAR(255),
		final_weight INT,
		education VARCHAR(255),
		education_num INT,
		marital_status VARCHAR(255),
		occupation VARCHAR(255),
		relationship VARCHAR(255),
		race VARCHAR(255),
		sex VARCHAR(255),
		capital_gain INT,
		capital_loss INT,
		hours_per_week INT,
		native_country VARCHAR(255),
		class VARCHAR(255)
	);
	```

3. Create the table for the anonymized data

	```
	CREATE TABLE adults_anonymized (
		age_from INT,
		age_to INT,
		workclass VARCHAR(255),
		education_num_from INT,
		education_num_to INT,
		marital_status VARCHAR(255),
		occupation VARCHAR(255),	
		race VARCHAR(255),
		sex VARCHAR(255),
		native_country VARCHAR(255),
		class VARCHAR(255)
	);
	```


4. Create a user for the application

	```
	CREATE USER 'anon_module' IDENTIFIED BY '<<password>';

	GRANT SELECT ON <<database_name>>.adults TO 'anon_module';
	GRANT SELECT, INSERT ON <<database_name>>.adults_anonymized TO 'anon_module';
	```

5. Load the adults data into MySQL

	```
	LOAD DATA LOCAL INFILE '/tmp/adults.data' 
	INTO TABLE adults 
	FIELDS TERMINATED BY ',' 
	LINES TERMINATED BY '\r\n';
	```




# Algorithms

## Mondrian

The Mondrian implementation is based on the [Basic Mondiran repository of Qiyuan Gong](https://github.com/qiyuangong/Basic_Mondrian).

## Datafly
