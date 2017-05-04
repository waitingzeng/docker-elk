#!/usr/bin/env python
#coding=utf8
from gevent import monkey
monkey.patch_all()

import MySQLdb
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import logging

db = {
    'host': 'vpc-product.cukhkd3vy9hv.us-east-1.rds.amazonaws.com',
    'post': 3306,
    'db': 'product_db_v2',
    'user': 'prod_product',
    'password': 'secret008'
}
db_conn = MySQLdb.connect(connect_timeout=30, **db)
cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
es = Elasticsearch()


def sync_table(table_id):
    try:
        sql_last_value = file('.last_sync_id_%s' % table_id).read()
    except:
        sql_last_value = 0

    last_id = 0
    while True:
        sql = "SELECT * FROM product_%s WHERE p_id > %s and update_time > %s order by p_id LIMIT 10000" % (table_id, last_id, sql_last_value)
        print sql
        cursor.execute(sql)
        had = 0
        for item in cursor.fetchall():
            yield item
            last_id = item['p_id']
            had = True
        if not had:
            break


def sync_tables():
    for i in range(200):
        for item in sync_table(i):
            yield item


def send_to_es():
    actions = []
    for item in sync_tables():
        action = {
            "_index": "product",
            "_type": "amazon",
            "_source": item
        }

        actions.append(action)
        if len(actions) >= 100:
            logging.info("save to es %d", len(actions))
            res = helpers.bulk(es, actions, chunk_size=100, params={'request_timeout': 90})
            logging.info("save to es %d, res: %s", len(actions), res)
    logging.info("save to es %d", len(actions))
    res = helpers.bulk(es, actions, chunk_size=100, params={'request_timeout': 90})
    logging.info("save to es %d, res: %s", len(actions), res)
    

if __name__ == '__main__':
    send_to_es()
