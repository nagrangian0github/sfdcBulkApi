#!/bin/python3
# coding: utf-8

import json
import requests
import configparser


version_number = "v53.0"


def main():
    print("-----Salesforce Bulk API 2.0------")

    instance_url, access_token = get_oauth()
    jobid, state = post_job(instance_url, access_token)
    state = get_querybob(jobid, instance_url, access_token)
    while state in ["UploadComplete", "InProgress"]:
        state = get_querybob(jobid, instance_url, access_token)
        print(state)
    f = open('results.csv', 'w', encoding='UTF-8')
    f.write(get_queryresults(jobid, instance_url, access_token))
    f.close()


def get_queryresults(jobid, instance_url, access_token):
    authorization = 'Bearer ' + access_token
    headers = {'Accept': 'text/csv',
               'Authorization': authorization
               }
    url_queryjob = instance_url + "/services/data/" + version_number + "/jobs/query/" + jobid + "/results"
    res = requests.get(url_queryjob, headers=headers)
    res.encoding = res.apparent_encoding
    # 実行結果を判定
    if res.status_code != 200:
        raise Exception(f'code:{res.status_code}, text:{res.text}')

    # 実行結果の取得
    res_info = res.text
    return res_info


def get_querybob(jobid, instance_url, access_token):
    authorization = 'Bearer ' + access_token
    headers = {'content-type': 'application/json',
               'Authorization': authorization
               }
    url_queryjob = instance_url + "/services/data/" + version_number + "/jobs/query/" + jobid

    res = requests.get(url_queryjob, headers=headers)
    # 実行結果を判定
    if res.status_code != 200:
        raise Exception(f'code:{res.status_code}, text:{res.text}')

    # 実行結果の取得
    res_info = json.loads(res.text)
    state = res_info['state']
    return state


def post_job(instance_url, access_token):
    authorization = 'Bearer ' + access_token
    headers = {'content-type': 'application/json',
               'Authorization': authorization
               }
    payload = {
                "operation": "query",
                "query": "SELECT Id, Name, Description FROM Account",
                "contentType": "CSV",
                "columnDelimiter": "COMMA",
                "lineEnding": "CRLF"
    }
    url_query = instance_url + "/services/data/" + version_number + "/jobs/query"

    res = requests.post(url_query, headers=headers, data=json.dumps(payload))
    # 実行結果を判定
    if res.status_code != 200:
        raise Exception(f'code:{res.status_code}, text:{res.text}')

    # 実行結果の取得
    res_info = json.loads(res.text)
    jobid = res_info['id']
    state = res_info['state']
    return jobid, state


def get_oauth():
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
    
    # SalesForceへのセッション情報を取得
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    payload = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": pw
    }
    res = requests.post(url_auth, headers=headers, data=payload)
    
    # 実行結果を判定
    if res.status_code != 200:
        raise Exception(f'code:{res.status_code}, text:{res.text}')
    
    # 実行結果の取得
    res_info = json.loads(res.text)
    instance_url = res_info['instance_url']
    access_token = res_info['access_token']
    return instance_url, access_token


if __name__ == "__main__":
    main()
