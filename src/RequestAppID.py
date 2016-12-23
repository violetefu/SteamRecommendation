import requests
import sys
import getopt

try:
    opts, args = getopt.getopt(sys.argv[1:],'hk:',['help', 'key='])
except getopt.GetoptError:
    print 'python ./RequestAppID -k <key>'
    print '-h, --help:          print this help and exit'
    print '-k, --key:           steam web api key'
    sys.exit(2)
for opt, arg in opts:
    if opt in ('-h', '--help'):
        print 'python ./RequestAppID -k <key>'
        print '-h, --help:          print this help and exit'
        print '-k, --key:           steam web api key'
        sys.exit(1)
    elif opt in ('-k', '--key'):
        key = arg

with open('../input/steam_user_id.txt') as f:
	user_ids = f.readlines()

for user_id in user_ids:
	url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key='+key+'&steamid='+user_id+'&format=json'
	#r = requests.get(url, params)
	print url

