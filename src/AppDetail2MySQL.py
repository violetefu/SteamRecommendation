import sys, getopt
import numpy as np
import pandas as pd
import json, re
from datetime import datetime
from pandas.io.json import json_normalize
import sqlalchemy
from sqlalchemy_utils import database_exists, create_database
import pymysql

try:
	opts, args = getopt.getopt(sys.argv[1:],'hu:p:',['help', 'user=', 'password='])
except getopt.GetoptError:
    print 'python ./AppDetail2MySQL.py -u <MySQL user_id> -p <MySQL password>'
    print '-h, --help:          print this help and exit'
    print '-u, --user:          MySQL user id'
    print '-p, --password:      MySQL password'
    sys.exit(2)

for opt, arg in opts:
    if opt in ('-h', '--help'):
    	print 'python ./AppDetail2MySQL.py -u <MySQL user_id> -p <MySQL password>'
        print '-h, --help:          print this help and exit'
        print '-u, --user:          MySQL user id'
        print '-p, --password:      MySQL password'
        sys.exit(1)
    elif opt in ('-u', '--user'):
        sql_user = arg
    elif opt in ('-p', '--password'):
        sql_pwd = arg

with open('../input/app_detail.txt', 'rb') as f:
	app_details = f.read().splitlines()

apps = {}
for app_detail in app_details:
	app_detail = json.loads(app_detail)
	apps.update(app_detail)

# Extract information from json (dict) format to pandas dataframe
df = json_normalize(apps.values())

df['steam_appid'] = apps.keys()
df['steam_appid'] = df['steam_appid'].astype(int)
df['name'] = df['data.name']
df['type'] = df['data.type']

# currency: 'USD', 'PHP'; all change to USD
df['initial_price'] = df.apply(lambda x: x['data.price_overview.initial']*0.02 if x['data.price_overview.currency'] == 'PHP' else x['data.price_overview.initial'], axis = 1)
df['initial_price'] = df['initial_price'] / 100.0
df['initial_price'] = df.apply(lambda x: 0 if x['data.is_free']==True else x['initial_price'], axis = 1)
# set initial_price as 0 when data.is_free = True
#df[pd.notnull(df[['data.is_free']]).any(axis = 1)][['data.is_free','initial_price']]
#df.loc[df['data.is_free'],['initial_price']] = 0

def dateConvert(date):
    try:
        if re.search(',', date) == None:
            return datetime.strptime(date, '%b %Y')
        elif date[0].isalpha():
            return datetime.strptime(date, '%b %d, %Y')
        else:
            return datetime.strptime(date, '%d %b, %Y')
    except (ValueError, TypeError) as e:
        print e, date
        return pd.NaT

df['release_date'] = df['data.release_date.date'].map(lambda x: dateConvert(x))
df['score'] = df['data.metacritic.score']
df['recommendation'] = df['data.recommendations.total']
df['windows'] = df['data.platforms.windows'].map(lambda x: 1 if x else 0)
df['mac'] = df['data.platforms.mac'].map(lambda x: 1 if x else 0)
df['linux'] = df['data.platforms.linux'].map(lambda x: 1 if x else 0)
df['header_image'] = df['data.header_image']

# Remove invalid steam_appid with no product information
df = df[pd.notnull(df['name'])]

df = df[['steam_appid','name','type','initial_price','release_date','score','recommendation','windows','mac','linux','header_image']].sort(['steam_appid'])
df.to_csv('../output/steam_app_info.csv', encoding='utf8', index = False)

#####################
### save to MySQL ###
#####################
engine = sqlalchemy.create_engine('mysql+pymysql://'+sql_user+':'+sql_pwd+'@127.0.0.1/game_recommendation?charset=utf8mb4')
if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))

engine.execute('''
    CREATE TABLE IF NOT EXISTS tbl_steam_app
    (
        steam_appid INT,
        name VARCHAR(500) CHARACTER SET utf8mb4,
        type VARCHAR(15),
        initial_price FLOAT,
        release_date VARCHAR(20),
        score INT,
        recommendation INT,
        windows BOOLEAN,
        mac BOOLEAN,
        linux BOOLEAN,
        header_image VARCHAR(100)
    );
    ''')

df.to_sql(name='tbl_steam_app', con=engine, if_exists = 'replace', index=False, flavor='mysql')

