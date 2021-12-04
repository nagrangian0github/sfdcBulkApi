#!/bin/python3

import json
import requests
import configparser

# --------------------------------------------------
# configparserの宣言とiniファイルの読み込み
# --------------------------------------------------
config_ini = configparser.ConfigParser()
config_ini.read('config.ini', encoding='utf-8')

my_domain = config_ini['DEFAULT']['my_domain']
url_auth = "https://" + my_domain + ".my.salesforce.com/services/oauth2/token"
client_id = config_ini['DEFAULT']['client_id']
client_secret = config_ini['DEFAULT']['client_secret']
username = config_ini['DEFAULT']['username']
pw = config_ini['DEFAULT']['pw']

#SalesForceへのセッション情報を取得
headers={'content-type':'application/x-www-form-urlencoded'}
payload={
    "grant_type": "password",
    "client_id": client_id,
    "client_secret": client_secret,
    "username": username,
    "password": pw
}
res=requests.post(url_auth,headers=headers,data=payload)

# 実行結果を判定
if res.status_code!=200:
    raise Exception(f'code:{res.status_code}, text:{res.text}')

# 実行結果の取得
session=requests.Session()
res_info=json.loads(res.text)
instance_url=res_info['instance_url']
access_token=res_info['access_token']
print(instance_url)
print(access_token)



