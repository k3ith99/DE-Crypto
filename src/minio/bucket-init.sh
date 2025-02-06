#!/bin/bash
mc alias set local ${ADDRESS} ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
mc admin info local 
mc mb test