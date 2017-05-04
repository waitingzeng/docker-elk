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

db = {
    'host': 'vpc-product.cukhkd3vy9hv.us-east-1.rds.amazonaws.com',
    'port': 3306,
    'db': 'product_db_v2',
    'user': 'prod_product',
    'passwd': 'secret008'
}
db_conn = MySQLdb.connect(connect_timeout=30, **db)
cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
es = Elasticsearch(hosts=['http://elastic:secret08@127.0.0.1:9200'])

class Global(object):
    total = 0

def sync_table(table_id):
    try:
        sql_last_value = file('.last_sync_id_%s' % table_id).read()
    except:
        sql_last_value = 0

    last_id = 0
    while True:
        sql = "SELECT * FROM product_%s WHERE p_id > %s and update_time > %s order by p_id LIMIT 10000" % (table_id, last_id, sql_last_value)
        logging.info("run sql: %s", sql)
        cursor.execute(sql)
        had = 0
        for item in cursor.fetchall():
            try:
                item['product_category'] = json.loads(item['product_category'])
            except:
                item['product_category'] = []
            item['product_name'] = item['product_name'].decode('utf8', 'ignore')
            yield item
            last_id = item['p_id']
            had = True
            sql_last_value = item['update_time']
        f = file('.last_sync_id_%s' % table_id, 'w')
        f.write(str(sql_last_value))
        f.close()
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
            "_id": item['product_sku'],
            "_timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "_version": "1",
            "_source": item
        }

        actions.append(action)
        if len(actions) >= 1000:
            logging.info("save to es %d", len(actions))
            res = helpers.bulk(es, actions, chunk_size=1000, params={'request_timeout': 90})
            Global.total += res[0]
            logging.info("save to es %d, res: %s, total: %d", len(actions), res, Global.total)
            actions = []

    logging.info("save to es %d", len(actions))
    res = helpers.bulk(es, actions, chunk_size=100, params={'request_timeout': 90})
    Global.total += res[0]
    logging.info("save to es %d, res: %s, total: %d", len(actions), res, Global.total)


if __name__ == '__main__':
    send_to_es()
