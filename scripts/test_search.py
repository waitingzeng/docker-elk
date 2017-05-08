#!/usr/bin/env python
# coding=utf8
from gevent import monkey
monkey.patch_all()
from setup_logger import setup_logger
from datetime import datetime
setup_logger()
import requests
from gevent import pool
import random

g_pool = pool.Pool(10)


def search(query):
    query = {
        "query": {
            "query_string": {
                "query": query
            }
        },
        "size": 0,
        "aggs": {
            "2": {
                "terms": {
                    "field": "product_category"
                }
            }
        }
    }
    requests.get(
        'http://elastic:changeme@127.0.0.1:9200/product/amazon/_search?', json=query)

while True:
    g_pool.spawn(search, str(random.randint(0, 10000000)))

g_pool.join()
