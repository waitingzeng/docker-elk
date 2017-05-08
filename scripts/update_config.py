#!/usr/bin/env python
#coding=utf8
import sys
import os
import requests
import json

templates_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/elasticsearch/templates'


for basename in os.listdir(templates_dir):
    if not basename.endswith('.json'):
        continue
    fname = templates_dir + '/' + basename
    content = json.loads(file(fname).read())
    print content
    res = requests.put('http://elastic:changeme@127.0.0.1:9200/%s' % basename[:-4], json=content)
    print res, res.content
