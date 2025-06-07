from rdflib import Graph

g = Graph()

g.parse("graph_model.ttl")

print(g.serialize(format="turtle"))