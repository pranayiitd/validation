from py2neo import neo4j, cypher

graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
query = "START n=node(1) RETURN n"

def print_row(row):
        print(row[0])

cypher.execute(graph_db, query, row_handler=print_row("pranay"))
