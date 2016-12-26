import requests, json, os, random, time, itertools
import sys, getopt
from datetime import datetime
from multiprocessing import Pool

def split_list(lst_long, n):
	lst_splitted = []
	if len(lst_long) % n == 0:
		totalBatches = len(lst_long) / n
	else:
		totalBatches = len(lst_long) / n + 1
	for i in range(totalBatches):
		lst_splitted.append(lst_long[i*n:(i+1)*n])
	return lst_splitted

def show_work_status(singleCount, totalCount, currentCount=0):
    currentCount += singleCount
    percentage = 1. * currentCount / totalCount * 100
    status = '+' * int(percentage) + '-' * (100 - int(percentage))
    sys.stdout.write('\rStatus: [{0}] {1:.2f}%'.format(status, percentage))
    sys.stdout.flush()
    if percentage >= 100:
        print '\n'

def worker(lst_steam_id, key):
    dic_temp = {}
    for user_id in lst_steam_id:
        base_url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/'
        params = {
            'key': key,
            'steamid': user_id,
            'format': 'json'
        }
        r = requests.get(base_url, params = params)
        user_inventory = r.json().get('response').get('games')
        dic_temp.update({user_id:user_inventory})
        time.sleep(.1)
    return dic_temp

def worker_star(lst_steam_id_key):
    return worker(*lst_steam_id_key)

if __name__ == '__main__':
    nfolds = 1

    try:
        opts, args = getopt.getopt(sys.argv[1:],'hk:n:',['help', 'key=', 'nfolds='])
    except getopt.GetoptError:
        print 'python ./RequestAppID -k <key>'
        print '-h, --help:          print this help and exit'
        print '-k, --key:           steam web api key'
        print '-n, --nfolds:        number of threads'
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print 'python ./RequestAppID -k <key> -n <nfolds>'
            print '-h, --help:          print this help and exit'
            print '-k, --key:           steam web api key'
            print '-n, --nfolds:        number of threads'
            sys.exit(1)
        elif opt in ('-k', '--key'):
            key = arg
        elif opt in ('-n', '--nfolds'):
            nfolds = int(arg)

    path_user_id = '../input/steam_user_id.txt'
    path_user_inventory = '../output/%s_user_inventory.json' % (datetime.today().isoformat()[:10])

    with open('../input/steam_user_id.txt', 'rb') as f:
	   set_user_ids = set(f.read().splitlines())

    if os.path.exists(path_user_inventory):
        with open(path_user_inventory, 'rb') as f:
            json_crawled_data = json.load(f)
            set_crawled_user_id = set(json_crawled_data.keys())
    else:
        json_crawled_data = {}
        set_crawled_user_id = set([])

    total_count = len(set_user_ids)
    current_count = len(set_crawled_user_id)
    lst_remaining_id = list(set_user_ids - set_crawled_user_id)

    p = Pool(nfolds)
    for lst_steam_id in split_list(lst_remaining_id, 1000):
        lst_temp_dic = p.map(worker_star, itertools.izip(split_list(lst_steam_id, 250), itertools.repeat(key)))
        for dic_temp in lst_temp_dic:
            json_crawled_data.update(dic_temp)

        with open(path_user_inventory,'wb') as f:
            json.dump(json_crawled_data, f)

        show_work_status(len(lst_steam_id), total_count, current_count)
        current_count += len(lst_steam_id)
