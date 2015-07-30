# build_index.py

import csv
import urllib2
from elasticsearch import Elasticsearch

FILE_URL = "https://raw.githubusercontent.com/sayantansatpati/data-acquisition-storage/master/scaling-up/search/train-titanic.csv"

ES_HOST = {"host" : "localhost", "port" : 9200}

INDEX_NAME = 'titanic'
TYPE_NAME = 'passenger'
ID_FIELD = 'passengerid'

response = urllib2.urlopen(FILE_URL)
csv_file_object = csv.reader(response)
 
header = csv_file_object.next()
header = [item.lower() for item in header]
header.append('age_num')
header.append('fare_num')
print header

bulk_data = [] 

for row in csv_file_object:
    data_dict = {}
    for i in range(len(row)):
        data_dict[header[i]] = row[i]
	try:
		if header[i] == 'age':
			data_dict['age_num'] = float(row[i]) 
		if header[i] == 'fare':
                	data_dict['fare_num'] = float(row[i])
	except Exception as e:
		print e
		pass
    op_dict = {
        "index": {
        	"_index": INDEX_NAME, 
        	"_type": TYPE_NAME, 
        	"_id": data_dict[ID_FIELD]
        }
    }
    bulk_data.append(op_dict)
    bulk_data.append(data_dict)

#print bulk_data


# create ES client, create index
es = Elasticsearch(hosts = [ES_HOST])

if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))

# since we are running locally, use one shard and no replicas
request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}

print("deleting '%s' index..." % (INDEX_NAME))
res = es.indices.delete(index = INDEX_NAME, ignore=[400, 404])
print(" response: '%s'" % (res))

print("creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print(" response: '%s'" % (res))

# bulk index the data
print("bulk indexing...")
res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)

# sanity check
res = es.search(index = INDEX_NAME, size=2, body={"query": {"match_all": {}}})
print(" response: '%s'" % (res))

print("results:")
for hit in res['hits']['hits']:
    print(hit["_source"])


