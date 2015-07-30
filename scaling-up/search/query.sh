#!/bin/bash

#  Data Used: Kaggle Titanic Data (Training Set: https://www.kaggle.com/c/titanic/data)
#  Used Python to Index Data (Extra fields added to index as number: age & fare)
#  GIT: https://github.com/sayantansatpati/data-acquisition-storage/tree/master/scaling-up/search

#  ssh root@198.23.94.10
#  Password: FkvtR6Bn


# Aggregation: Survived/Not-Survived By sex (male/female)

curl -XPOST "http://localhost:9200/titanic/_search?search_type=count&pretty" -d'
{
"aggs" : { 
        "sex" : { 
            "terms" : {
              "field" : "sex" 
            },
            "aggs": { 
                "survived": {
                   "terms": {
                      "field": "survived"
                   }
                }
            }
        }
    }
}'

# Aggregation: Survived/Not-Survived By Port of Embarkation (C = Cherbourg; Q = Queenstown; S = Southampton) & Sex (male/female)

curl -XPOST "http://localhost:9200/titanic/_search?search_type=count&pretty" -d'
{
"aggs" : { 
        "embarked" : { 
            "terms" : {
              "field" : "embarked" 
            },
            "aggs": { 
                "sex": {
                   "terms": {
                      "field": "sex"
                   },
                   "aggs": { 
                        "survived": {
                           "terms": {
                              "field": "survived"
                           }
                        }
                    }
                }
            }
        }
    }
}'

# Aggregation: Survived/Not-Survived By Passenger Class (1 = 1st; 2 = 2nd; 3 = 3rd) & Sex (male/female)

curl -XPOST "http://localhost:9200/titanic/_search?search_type=count&pretty" -d'
{
"aggs" : { 
        "pclass" : { 
            "terms" : {
              "field" : "pclass" 
            },
            "aggs": { 
                "sex": {
                   "terms": {
                      "field": "sex"
                   },
                   "aggs": { 
                        "survived": {
                           "terms": {
                              "field": "survived"
                           }
                        }
                    }
                }
            }
        }
    }
}'

# Range Query on Age: Showing number of passengers & statistics (min, max, avg, sum) per Age Bucket

curl -XPOST "http://localhost:9200/titanic/_search?search_type=count&pretty" -d'
{
    "aggs" : {
        "age_ranges" : {
            "range" : {
                "field" : "age_num",
                "keyed" : true,
                "ranges" : [
                    { "from" : 0, "to" : 10 },
                    { "from" : 11, "to" : 20 },
                    { "from" : 21, "to" : 30 },
                    { "from" : 31, "to" : 40 },
                    { "from" : 41, "to" : 50 },
                    { "from" : 51, "to" : 60 },
                    { "from" : 61 }
                ]
            },
            "aggs" : {
                "age_stats" : {
                    "stats" : { "field" : "age_num" }
                }
            }
        }
    }
}'

# Range Query on Age: Showing number of survivors grouped by sex & age range

curl -XPOST "http://localhost:9200/titanic/_search?search_type=count&pretty" -d'
{
    "aggs" : {
        "age_ranges" : {
            "range" : {
                "field" : "age_num",
                "ranges" : [
                    { "from" : 0, "to" : 10 },
                    { "from" : 11, "to" : 20 },
                    { "from" : 21, "to" : 30 },
                    { "from" : 31, "to" : 40 },
                    { "from" : 41, "to" : 50 },
                    { "from" : 51, "to" : 60 },
                    { "from" : 61 }
                ]
            },
            "aggs": { 
                "sex": {
                   "terms": {
                      "field": "sex"
                   },
                   "aggs": { 
                        "survived": {
                           "terms": {
                              "field": "survived"
                           }
                        }
                    }
                }
            }
        }
    }
}'

