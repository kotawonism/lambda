#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime
import csv
import pprint
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, date, timedelta

# ライブラリパスにカレントディレクトリのlibを追加
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))

BASE_DIRPATH = os.path.dirname(os.path.abspath(__file__))
FILEDOWNLOADPATH = '/tmp/out.csv'

# 昨日日付を取得
yesterday = datetime.strftime(datetime.today() - timedelta(days=1),'%Y/%m/%d/')

S3_OUTPUT = 'S3_path' + yesterday
S3_BUCKET ='S3_bucketname'
DATABASE = 'Athena_DB＿name'
TABLE = 'Athena_table_name'
print(S3_OUTPUT)

# number of retries
RETRY_COUNT = 300

class AtenaResult:
    STATUS_QUEUED    = "QUEUED"
    STATUS_RUNNING   = "RUNNING"
    STATUS_SUCCEEDED = "SUCCEEDED"
    STATUS_FAILED    = "FAILED"
    STATUS_CANCELLED = "CANCELLED"
    STATUS_TIMEOUT   = "TIMEOUT" # AWSのAPIにはないこのプログラムの為のステータス

    def __init__(self, id):
        self.id = id
        self.status = ""

    def getResultCsvFilename(self):
        return self.id + ".csv"

    def isSuccess(self):
        return self.status == self.STATUS_SUCCEEDED

def drop_table():
    print ("== DROP TABLE ==")
    return query_execute("DROP TABLE IF EXISTS `"+ TABLE + "`")

def create_table():
    print ("== CREATE TABLE ==")

    # sqlのために、昨日の日付を分解する
    year  = yesterday.split('/')[0]
    month = yesterday.split('/')[1]
    day   = yesterday.split('/')[2]

    filepath = os.path.join(BASE_DIRPATH, "sql", "create_table.sql")
    with open(filepath) as f:
        query = f.read();

        # S3のLOCATIONの日付箇所置換
        # format関数を使うと正規表現の {} を置換場所と
        # 判定してしまうので %s 方式でのフォーマットを使う
        query = query % (year, month, day)
        return query_execute(query)

def aggregate():
    print ("== LOG AGGREGATE ==")

    filepath = os.path.join(BASE_DIRPATH, "sql", "log_analytics.sql")
    with open(filepath) as f:
        query = f.read();
        return query_execute(query)

def query_execute(query):
    # クエリの実行
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={ 'Database': DATABASE },
        ResultConfiguration={ 'OutputLocation': S3_OUTPUT, }
    )
    result = AtenaResult(response['QueryExecutionId'])
    print(result.id)

    # RETRY_COUNT の終わりまで繰り返す
    for i in range(1, 1 + RETRY_COUNT):
        
        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=result.id)
        result.status = query_status['QueryExecution']['Status']['State']

        # クエリ成功
        if result.status == result.STATUS_SUCCEEDED:
            print("STATUS:" + result.status)
            return result

        # クエリ失敗
        elif result.status == result.STATUS_FAILED or result.status == result.STATUS_CANCELLED:
            return result
            # raise Exception("STATUS:" + query_execution_status)

        # その他(実行中)
        else:
            print("STATUS:" + result.status)
            time.sleep(i)

    ### 再試行回数を超えた場合
    else:
        client.stop_query_execution(QueryExecutionId=result.id)
        result.status = result.STATUS_TIMEOUT
        return result
        # raise Exception('TIME OVER')



# main
def lambda_handler(event, context):

    ### Athena
    result = drop_table();
    if not result.isSuccess():
        print ("error: drop table " + result.status)
        exit(1)

    result = create_table()
    if not result.isSuccess():
        print ("error: create table " + result.status)
        exit(1)

    result = aggregate()
    if not result.isSuccess():
        print ("error: log aggregate " + result.status)
        exit(1)

if __name__ == "__main__":
    # ローカルで手動実行するための設定
    e = {}
    c = "test"
    lambda_handler(e, c)
