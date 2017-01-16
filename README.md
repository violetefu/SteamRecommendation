# SteamRecommendation
RequestAppID.py: a webcrawler to collect inventory information Steam users through Steam Web API (https://developer.valvesoftware.com/wiki/Steam_Web_API#GetOwnedGames_.28v0001.29) 
$ python ./RequestAppID.py -k <Steam Web API Key> -n <num_threads>

RequestAppDetail.py: a webcrawler to collect app information through Steam API (https://wiki.teamfortress.com/wiki/User:RJackson/StorefrontAPI#appdetails)
$ python ./RequestAppDetail.py

AppDetail2MySQL.py: to extract basic app information from the webcrawler, and save them in MySQL or Hive
$ python ./AppDetail2MySQL.py -u <MySQL user_id> -p <MySQL password>

HiveRelated.sql: Hive related commands

GameRecommend_ALS.py: to implement the ALS algorithm in Spark MLlib to establish a game recommendation system
$ python python ./GameRecommend_ALS.py -u <MySQL user_id> -p <MySQL password>

./website/run.py: a demo for the game recommendation system (see details in readme.txt)
$ python ./run.py -u <MySQL user_id> -p <MySQL password>
