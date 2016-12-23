import requests
import sys
import getopt
import json

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

with open('../input/steam_user_id.txt', 'r') as f:
	user_ids = f.read().splitlines()

with open('../input/user_inventory.txt', 'w') as f:
    for user_id in user_ids:
        url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key='+key+'&steamid='+user_id+'&format=json'
        response = requests.get(url).json()
        json.dump({user_id:response.get('response').get('games')}, f)
        f.write('\n')

