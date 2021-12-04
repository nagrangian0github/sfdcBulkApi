#!/bin/python3
# coding: utf-8

import json
import requests
import configparser
import os


def main():
    print("-----Salesforce Bulk API 2.0------")
    sp = SalsforceApi()
    sp.post_job()
    state = sp.get_querybob()
    while state in ["UploadComplete", "InProgress"]:
        state = sp.get_querybob()
    print('Job State：' + state)
    sp.get_queryresults()
    sp.write_results()
    if sp.delete_queryjob() == 204:
        print('Job Delete： Success')


class SalsforceApi:

    def __init__(self):
        self.jobid = ''
        self.results = ''

        # --------------------------------------------------
        # configparserの宣言とiniファイルの読み込み
        # --------------------------------------------------
        config_ini = configparser.ConfigParser()
        config_ini.read(os.path.dirname(__file__) + '/config.ini', encoding='utf-8')

        my_domain = config_ini['DEFAULT']['my_domain']
        url_auth = "https://" + my_domain + ".my.salesforce.com/services/oauth2/token"
        client_id = config_ini['DEFAULT']['client_id']
        client_secret = config_ini['DEFAULT']['client_secret']
        username = config_ini['DEFAULT']['username']
        pw = config_ini['DEFAULT']['pw']
        self.soql = config_ini['DEFAULT']['soql']
        self.version_number = config_ini['DEFAULT']['version_number']
        self.output_file = config_ini['DEFAULT']['output_file']

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
        self.instance_url = res_info['instance_url']
        self.access_token = res_info['access_token']

        print('接続ID：' + username)
        print('API Version：' + self.version_number)
        print('instance_url：' + self.instance_url)

    def write_results(self):
        f = open(self.output_file, 'w', newline='\n', encoding='UTF-8')
        f.write(self.results)
        f.close()
        print('Output File：' + self.output_file)

    def delete_queryjob(self):
        authorization = 'Bearer ' + self.access_token
        headers = {'content-type': '',
                   'Authorization': authorization
                   }
        url_queryjob = self.instance_url + "/services/data/" + self.version_number + "/jobs/query/" + self.jobid

        res = requests.delete(url_queryjob, headers=headers)
        # 実行結果を判定
        if res.status_code != 204:
            raise Exception(f'code:{res.status_code}, text:{res.text}')

        # 実行結果の取得
        res_info = res.status_code
        return res_info

    def get_queryresults(self):
        authorization = 'Bearer ' + self.access_token
        headers = {'Accept': 'text/csv',
                   'Authorization': authorization
                   }
        url_queryjob \
            = self.instance_url + "/services/data/" + self.version_number + "/jobs/query/" + self.jobid + "/results"
        res = requests.get(url_queryjob, headers=headers)
        res.encoding = res.apparent_encoding
        # 実行結果を判定
        if res.status_code != 200:
            raise Exception(f'code:{res.status_code}, text:{res.text}')

        # 実行結果の取得
        res_info = res.text
        self.results = res_info

    def get_querybob(self):
        authorization = 'Bearer ' + self.access_token
        headers = {'content-type': 'application/json',
                   'Authorization': authorization
                   }
        url_queryjob = self.instance_url + "/services/data/" + self.version_number + "/jobs/query/" + self.jobid

        res = requests.get(url_queryjob, headers=headers)
        # 実行結果を判定
        if res.status_code != 200:
            raise Exception(f'code:{res.status_code}, text:{res.text}')

        # 実行結果の取得
        res_info = json.loads(res.text)
        state = res_info['state']
        return state

    def post_job(self):
        authorization = 'Bearer ' + self.access_token
        headers = {'content-type': 'application/json',
                   'Authorization': authorization
                   }
        payload = {
            "operation": "query",
            "query": self.soql,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "CRLF"
        }
        url_query = self.instance_url + "/services/data/" + self.version_number + "/jobs/query"

        res = requests.post(url_query, headers=headers, data=json.dumps(payload))
        # 実行結果を判定
        if res.status_code != 200:
            raise Exception(f'code:{res.status_code}, text:{res.text}')

        # 実行結果の取得
        res_info = json.loads(res.text)
        self.jobid = res_info['id']
        state = res_info['state']
        return state


if __name__ == "__main__":
    main()
