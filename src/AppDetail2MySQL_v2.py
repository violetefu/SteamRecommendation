import os, sys, getopt
import numpy as np
import pandas as pd
import json, re
from datetime import datetime
import sqlalchemy
from sqlalchemy_utils import database_exists, create_database
import pymysql

try:
	opts, args = getopt.getopt(sys.argv[1:],'hu:p:',['help', 'user=', 'password='])
except getopt.GetoptError:
    print 'python ./AppDetail2MySQL_v2.py -u <MySQL user_id> -p <MySQL password>'
    print '-h, --help:          print this help and exit'
    print '-u, --user:          MySQL user id'
    print '-p, --password:      MySQL password'
    sys.exit(2)

for opt, arg in opts:
    if opt in ('-h', '--help'):
    	print 'python ./AppDetail2MySQL_v2.py -u <MySQL user_id> -p <MySQL password>'
        print '-h, --help:          print this help and exit'
        print '-u, --user:          MySQL user id'
        print '-p, --password:      MySQL password'
        sys.exit(1)
    elif opt in ('-u', '--user'):
        sql_user = arg
    elif opt in ('-p', '--password'):
        sql_pwd = arg

# set file path
path_app_info = '../output/steam_app_info.csv'
path_app_detail = '../input/app_detail.txt'

if not os.path.exists(path_app_info):
    with open(path_app_detail, 'rb') as f:
        dic_app_detail = {'name':{}, 'type': {}, 'initial_price': {}, 'release_date': {}, 'score': {}, 'recommendation': {}, 'windows':{}, 'mac':{}, 'linux':{}, 'header_image':{}}
        lst_raw_string = f.readlines()

        for raw_string in lst_raw_string:
            app_detail = json.loads(raw_string).values()[0]
            if app_detail.get('success'):
                app_data = app_detail.get('data')
                app_steam_id = app_data.get('steam_appid')
                app_name = app_data.get('name')
                app_type = app_data.get('type')
                
                app_initial_price = app_data.get('price_overview', {}).get('initial') # some records don't have this item, should set default value
                if app_data.get('is_free'):
                    app_initial_price = 0
                if app_data.get('price_overview',{}).get('currency') == 'PHP':
                    app_initial_price = app_initial_price * 0.02
                
                if app_data.get('release_date',{}).get('coming_soon') == False:
                    app_release_date = app_data.get('release_date',{}).get('date')
                    if not app_release_date == '':
                        if re.search(',', app_release_date) == None:
                            app_release_date = datetime.strptime(app_release_date, '%b %Y')
                        elif app_release_date[0].isalpha():
                            app_release_date = datetime.strptime(app_release_date, '%b %d, %Y')
                        else:
                            app_release_date = datetime.strptime(app_release_date, '%d %b, %Y')
                            
                app_score = app_data.get('metacritic',{}).get('score')
                app_recommendation = app_data.get('recommendations',{}).get('total')
                
                for (key, val) in app_data.get('platforms').items():
                    if val:
                        dic_app_detail[key].update({app_steam_id:1})
                app_header_image = app_data.get('header_image')

                dic_app_detail['name'].update({app_steam_id:app_name})
                dic_app_detail['type'].update({app_steam_id:app_type})
                dic_app_detail['initial_price'].update({app_steam_id:app_initial_price})
                dic_app_detail['release_date'].update({app_steam_id:app_release_date})
                dic_app_detail['score'].update({app_steam_id:app_score})
                dic_app_detail['recommendation'].update({app_steam_id:app_recommendation})
                dic_app_detail['header_image'].update({app_steam_id:app_header_image})
    df_steam_app = pd.DataFrame(dic_app_detail)
    df_steam_app.index.name = 'steam_appid'
    df_steam_app['initial_price'] = df_steam_app.initial_price / 100.0
    df_steam_app['windows'] = df_steam_app.windows.fillna(0)
    df_steam_app['mac'] = df_steam_app.mac.fillna(0)
    df_steam_app['linux'] = df_steam_app.linux.fillna(0)
    df_steam_app = df_steam_app[['name', 'type', 'initial_price', 'release_date', 'score', 'recommendation', 'windows', 'mac', 'linux', 'header_image']]
    df_steam_app.reset_index(inplace=True)
    df_steam_app.to_csv(path_app_info,encoding='utf8',index=False)
    
else:
    print path_app_info, 'already exists'
    df_steam_app = pd.read_csv(path_app_info)

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

df_steam_app.to_sql(name='tbl_steam_app', con=engine, if_exists = 'replace', index=False, flavor='mysql')

#engine.execute('''
#    LOAD DATA INFILE '%s' INTO TABLE `tbl_steam_app` 
#    FIELDS TERMINATED BY ','
#    OPTIONALLY ENCLOSED BY '"'
#    LINES TERMINATED BY '\n'
#    IGNORE 1 LINES
#    (@steam_appid, @name, @type, @initial_price, @release_date, @score, @recommendation, @windows, @mac, @linux, @header_image)
#    SET
#    steam_appid = nullif(@steam_appid, ''),
#    name = nullif(@name, ''),
#    type = nullif(@type, ''),
#    initial_price = nullif(@initial_price,''),
#    release_date = nullif(@release_date,''),
#    score = nullif(@score,''), 
#    recommendation = nullif(@recommendation, ''),
#    windows = nullif(@windows, ''),
#    mac = nullif(@mac, ''),
#    linux = nullif(@linux, ''),
#    header_image = nullif(@header_image, '');
#    ''' % path_app_info)


