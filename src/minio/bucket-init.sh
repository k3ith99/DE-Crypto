#!/bin/bash
#sleep 5
mc alias set local $ADDRESS $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc alias list
mc admin info local 
mc mb local/bucket1
mc admin accesskey create local/
#for i in BTC ETH LTC DOGE BNB
#do
#    mc mb local/bucket1/$i
#done