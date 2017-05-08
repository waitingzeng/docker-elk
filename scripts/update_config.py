#!/usr/bin/env python
#coding=utf8
import sys
import os
import requests
import json

templates_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/elasticsearch/templates'


for fname in os.path.dirname(templates_dir):
    if not fname.endswith('.json'):
        continue
    fname = templates_dir + '/' + fname
    content = json.loads(file(fname).read())
    requests.put('http://elastic:changeme@127.0.0.1:9200', json=content)

