#!/usr/bin/env python
#coding=utf8
from gevent import monkey
monkey.patch_all()

import MySQLdb
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import logging
import json
from setup_logger import setup_logger
from datetime import datetime
setup_logger()
import sys
import time
from gevent import pool

db = {
    'host': 'vpc-product.cukhkd3vy9hv.us-east-1.rds.amazonaws.com',
    'port': 3306,
    'db': 'product_db_v2',
    'user': 'prod_product',
    'passwd': 'secret008'
}
db_conn = MySQLdb.connect(connect_timeout=30, **db)
cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
es = Elasticsearch(hosts=['http://elastic:changeme@127.0.0.1:9200'])

class Global(object):
    total = 0
    runtime = time.time()

def sync_table(table_id):
    try:
        sql_last_value = file('.last_sync_id_%s' % table_id).read()
    except:
        sql_last_value = 0

    last_id = 0
    while True:
        sql = "SELECT p_id, product_category, product_sku, product_name, update_time FROM product_%s WHERE p_id > %s and update_time > %s order by p_id LIMIT 10000" % (table_id, last_id, sql_last_value)
        logging.info("run sql: %s", sql)
        cursor.execute(sql)
        had = 0
        for item in cursor.fetchall():
            try:
                item['product_category'] = json.loads(item['product_category'])
            except:
                item['product_category'] = []
            item.pop('product_url', None)
            item['product_name'] = item['product_name'].decode('utf8', 'ignore')
            yield item
            last_id = item.pop('p_id')
            had = True
            sql_last_value = item.pop('update_time')
        f = file('.last_sync_id_%s' % table_id, 'w')
        f.write(str(sql_last_value))
        f.close()
        if not had:
            break


def sync_tables():
    for i in range(int(sys.argv[1]), int(sys.argv[2])):
        for item in sync_table(i):
            yield item


chunk_size = 1000
g_pool = pool.Pool(10)

def save(actions):
    logging.info("save to es %d", len(actions))
    res = helpers.bulk(es, actions, chunk_size=chunk_size, params={'request_timeout': 90})
    Global.total += res[0]
    logging.info("save to es %d, res: %s, total: %d", len(actions), res, Global.total)
    

def send_to_es():
    actions = []
    for item in sync_tables():
        action = {
            "_index": "product",
            "_type": "amazon",
            "_id": item.pop('product_sku'),
            "_source": item,
        }

        actions.append(action)
        if len(actions) >= chunk_size:
            new_actions, actions = actions, []
            g_pool.spawn(save, new_actions)

    save(actions)
    g_pool.join()

if __name__ == '__main__':
    send_to_es()
