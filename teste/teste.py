#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
#import numpy
import time
from time import clock
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-7-oracle/'
#os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-7-openjdk-amd64/'


from neo4j import GraphDatabase, INCOMING, Evaluation, OUTGOING, ANY

def TipoVar(v):
	tipo = type(v)
	if tipo == int:
	    return 'int'
	elif tipo == float:
	    return 'float'
	elif tipo == str:
	    return 'string'
	elif tipo == list:
	    return 'list'
	else:
	    return None

db = GraphDatabase('teste')

for node in db.nodes:
	for item in node.items():
		for a in item:
			print type(a).decode('ascii')

db.shutdown()

