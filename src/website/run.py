from flask import Flask, render_template
import sys, getopt
import json, sqlalchemy

app = Flask(__name__)

# set SQL user_id, passwd; change it if necessary
try:
	opts, args = getopt.getopt(sys.argv[1:],'hu:p:',['help', 'user=', 'password='])
except getopt.GetoptError:
    print 'python ./run.py -u <MySQL user_id> -p <MySQL password>'
    print '-h, --help:          print this help and exit'
    print '-u, --user:          MySQL user id'
    print '-p, --password:      MySQL password'
    sys.exit(2)

for opt, arg in opts:
    if opt in ('-h', '--help'):
    	print 'python ./run.py -u <MySQL user_id> -p <MySQL password>'
        print '-h, --help:          print this help and exit'
        print '-u, --user:          MySQL user id'
        print '-p, --password:      MySQL password'
        sys.exit(1)
    elif opt in ('-u', '--user'):
        sql_user = arg
    elif opt in ('-p', '--password'):
        sql_pwd = arg

# json.dump(dic_recommended, open(path_recommended_games, 'wb'), indent=3)
engine = sqlalchemy.create_engine('mysql+pymysql://'+sql_user+':'+sql_pwd+'@127.0.0.1/game_recommendation?charset=utf8mb4')

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!\n\nAppend /recommendation/<userid> to the current url\n\nSome availble userids: 76561198158086086, 76561198074188133, 76561198058088990\n\n"

@app.route('/recommendation/<userid>')
def recommendation(userid):
	result = engine.execute('''
		SELECT g0,g1,g2,g3,g4,g5,g6,g7,g8,g9 FROM tbl_recommended_games WHERE user_id=%s;
		''' % userid).first()


	lst_recommended_games = []
	for app_id in list(result):
		app_data = engine.execute('''
						SELECT name,initial_price,header_image FROM tbl_steam_app WHERE steam_appid = %s;
					''' % app_id).first()
		if app_data != None:
			lst_recommended_games.append(app_data)

	return render_template( 'recommendation.html',
							userid = userid,
							lst_recommended_games = lst_recommended_games)


if __name__ == '__main__':
	app.run(debug=True)