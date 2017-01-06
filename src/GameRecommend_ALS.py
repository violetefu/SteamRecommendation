import sys, getopt
import numpy as np
import pandas as pd
import json, math
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
import sqlalchemy
from sqlalchemy_utils import database_exists, create_database
import pymysql

try:
	opts, args = getopt.getopt(sys.argv[1:],'hu:p:',['help', 'user=', 'password='])
except getopt.GetoptError:
    print 'python ./GameRecommend_ALS.py -u <MySQL user_id> -p <MySQL password>'
    print '-h, --help:          print this help and exit'
    print '-u, --user:          MySQL user id'
    print '-p, --password:      MySQL password'
    sys.exit(2)

for opt, arg in opts:
    if opt in ('-h', '--help'):
    	print 'python ./GameRecommend_ALS.py -u <MySQL user_id> -p <MySQL password>'
        print '-h, --help:          print this help and exit'
        print '-u, --user:          MySQL user id'
        print '-p, --password:      MySQL password'
        sys.exit(1)
    elif opt in ('-u', '--user'):
        sql_user = arg
    elif opt in ('-p', '--password'):
        sql_pwd = arg

###########################
### Load and parse data ###
###########################
def parse_raw_string(raw_string):
    user_inventory = json.loads(raw_string)
    return user_inventory.items()[0]

conf = SparkConf().setAppName("GameRecommendation")
sc = SparkContext(conf=conf)
user_inventory_rdd = sc.textFile('../input/user_inventory.txt').map(parse_raw_string).zipWithIndex()

#################################
### Label encoder for user_id ###
#################################
def id_index(x):
    (user_id, lst_inventory), index = x
    return (index, user_id)

dic_id_index = user_inventory_rdd.map(id_index).collectAsMap()

######################
### Create ratings ###
######################
##### Normalize play_time (ratings) to 0~10
def create_rating(x):
    (user_id, lst_inventory), index = x
    if lst_inventory != None:
        return (index, [(i.get('appid'), i.get('playtime_forever')) for i in lst_inventory if i.get('playtime_forever') > 0])
    else:
        return (index, [])

##### ratings does not include new users, but ratings.count() get error without returning (index, [])
ratings = user_inventory_rdd.map(create_rating).flatMapValues(lambda x: x).map(lambda (index, (appid,playtime)): (index, appid, playtime))
#print ratings.count(), ratings.first()


##################################################
### Split data as traing, validation, and test ###
##################################################
#training_RDD, validation_RDD, test_RDD = ratings.randomSplit([6, 2, 2], seed=0L)
#validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
#test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

##############################
### Tunning ALS parameters ###
##############################
#seed = 5L
#iterations = 10
#regularization_parameter = 0.1 # 0.01
#ranks = [3, 5, 10, 12, 15, 18, 20, 22, 25, 28, 30]
#blocks = -1

#for rank in ranks:
#    model = ALS.train(training_RDD, rank = rank, blocks=blocks, seed=seed, lambda_=regularization_parameter, iterations=iterations)
#    predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
#    rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), r[2])).join(predictions)
#    error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
#    print 'For rank %s the RMSE is %s' % (rank, error)

###############################
### Test the selected model ###
###############################
#best_rank = 28
#model = ALS.train(training_RDD, best_rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)
#predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
#rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
#error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

#print 'For testing data the RMSE is %s' % (error)

##############################################################################
### Build the recommendation model using Alternating Least Squares (Final) ###
##############################################################################
seed = 5L
iterations = 10
regularization_parameter = 0.1
rank = 28
blocks = -1

model = ALS.train(ratings, rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)

###############################################
### Recommend top 10 products for each user ###
###############################################
dic_recommend = {'g0':{},'g1':{},'g2':{},'g3':{},'g4':{},'g5':{},'g6':{},'g7':{},'g8':{},'g9':{}}
for index in dic_id_index.keys():
    try:
        user_id = dic_id_index[index]
        lst_recommend = [i.product for i in model.recommendProducts(index, 10)]
        for k in range(10):
            dic_recommend['g'+str(k)].update({user_id:lst_recommend[k]})
    except:
        pass

df = pd.DataFrame(dic_recommend)
df.index.name = 'user_id'
df = df.reset_index()
df.to_csv('../output/game_recommended.csv', index = False)
#df.head()

#####################
### save to MySQL ###
#####################
engine = sqlalchemy.create_engine('mysql+pymysql://'+sql_user+':'+sql_pwd+'@127.0.0.1/game_recommendation?charset=utf8mb4')
if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))
df.to_sql(name='tbl_recommended_games', con=engine, if_exists = 'replace', index=False, flavor='mysql')
