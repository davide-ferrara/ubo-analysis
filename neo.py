from py2neo import Graph, Node
import time
import csv
import scipy.stats as stats
import numpy as np
import json

graph = Graph("bolt://localhost:7687/", auth=("neo4j", "12345678"))

data = {"name": "Davide"}
nodo = Node("TEST", **data)

graph.create(nodo)
