import requests
import time
import json

with open('../input/app_id.txt', 'r') as f:
	app_ids = f.read().splitlines()

with open('../input/app_detail.txt', 'w') as f:
    for app_id in app_ids:
        params = {'appids': app_id}
        url = 'http://store.steampowered.com/api/appdetails/'
        response = requests.get(url, params).json()
        json.dump(response, f)
        f.write('\n')
        time.sleep(2)

