#!/bin/sh

# スクリプトの場所にカレントにして、
# カレント配下のソースコードを全て圧縮しアップロード

cd `dirname $0`
zip -r ./deploy.zip ./*

echo deploy.zipが作成されました。アップロードを行なって適用してください。
echo https://ap-northeast-1.console.aws.amazon.com/lambda/home?region=ap-northeast-1#/functions/makeCloudWatchDesignatedAlarm?newFunction=true&tab=graph
