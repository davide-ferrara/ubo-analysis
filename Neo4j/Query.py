from py2neo import Graph, Node
import time
import csv
import scipy.stats as stats
import numpy as np
import json

# Connessione ai diversi dataset Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

def custom_json_serializer(obj):
    if isinstance(obj, Node):
        return dict(obj)
    raise TypeError("Type not serializable")

# Funzione per calcolare l'intervallo di confidenza
def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)  # Numero di osservazioni
    average_value = np.average(data)  # Valore medio
    stderr = stats.sem(data)  # Errore standard della media
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)  # Margine di errore
    return average_value, margin_of_error

# Funzione per misurare le performance di una query
def measure_query_performance(graph, query_func, percentage, iterations=30):
    subsequent_times = []
    
    for _ in range(iterations):
        start_time = time.time()  # Tempo di inizio
        query_func(graph)  # Esecuzione della query
        end_time = time.time()  # Tempo di fine
        execution_time = (end_time - start_time) * 1000  # Tempo di esecuzione in millisecondi
        subsequent_times.append(execution_time)
    
    # Calcolo dell'intervallo di confidenza per i tempi di esecuzione
    average, margin_of_error = calculate_confidence_interval(subsequent_times)
    average_subsequent_time = round(sum(subsequent_times) / len(subsequent_times), 2)
    
    return average_subsequent_time, average, margin_of_error

# Definizione della query 1
def query1(graph):
    company_name = 'Robertson Inc'
    query = f"""
    MATCH (c:Companies {{name: '{company_name}'}})
    RETURN c
    """
    result = graph.run(query).data()  # Esecuzione della query e recupero dei dati
    return result

# Definizione della query 2
def query2(graph):
    company_id = 2764
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    RETURN c, collect(a) as administrators
    """
    result = graph.run(query).data()  # Esecuzione della query e recupero dei dati
    return result

# Definizione della query 3
def query3(graph):
    company_id = 2764
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > 25
    RETURN c, collect(DISTINCT a) as administrators, collect(DISTINCT u) as ubos
    """
    
    result = graph.run(query).data()  # Esecuzione della query e recupero dei dati
    return result

# Definizione della query 4
def query4(graph):
    company_id = 2764
    start_date = "2016-07-01"
    end_date = "2024-07-01"
    ubo_percentage = 25
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > {ubo_percentage}
    OPTIONAL MATCH (c)-[:COMPANY_HAS_TRANSACTION]->(t:Transactions)
    WHERE t.date >= '{start_date}' AND t.date <= '{end_date}'
    RETURN c, collect(DISTINCT a) as administrators, collect(DISTINCT u) as ubos, sum(t.amount) as total_amount
    """
    result = graph.run(query).data()  # Esecuzione della query e recupero dei dati
    return result

def query5(graph):
    company_id = 2764
    currency = "EUR"
    date = "2003-01-01"

    # Query per recuperare l'azienda e i dettagli associati
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > 25
    OPTIONAL MATCH (c)-[:COMPANY_HAS_TRANSACTION]->(t:Transactions)
    WHERE t.currency = '{currency}' AND t.date >= '{date}'
    OPTIONAL MATCH (c)-[:COMPANY_HAS_SHAREHOLDER]->(s:Shareholders)
    RETURN c as company,
        collect(DISTINCT a) as administrators,
        collect(DISTINCT u) as ubos,
        sum(t.amount) as total_amount,
        collect(DISTINCT s) as shareholders
    """

    # Esecuzione della query
    result = graph.run(query).data()

    return result

def main():
    # Definizione dei grafi da analizzare
    graphs = {
        '100%': graph100,
        '75%': graph75,
        '50%': graph50,
        '25%': graph25
    }
    
    response_times_first_execution = {}  # Dizionario per i tempi di risposta della prima esecuzione
    average_response_times = {}  # Dizionario per i tempi di risposta medi

    for percentage, graph in graphs.items():
        print(f"\nAnalysis for percentage: {percentage}\n")

        # Query 1
        start_time = time.time()
        query_result = query1(graph)  # Esecuzione della query
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)  # Conversione in JSON
            print(f"Query 1 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified name\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)  # Tempo di esecuzione della prima query
        print(f"Response time (first execution - Query 1): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 1"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query1, percentage)
        print(f"Average time of 30 subsequent executions (Query 1): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 1): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 1"] = (average_subsequent_time, average, margin_of_error)

        # Query 2
        start_time = time.time()
        query_result = query2(graph)  # Esecuzione della query
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)  # Conversione in JSON
            print(f"Query 2 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified ID\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)  # Tempo di esecuzione della prima query
        print(f"Response time (first execution - Query 2): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 2"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query2, percentage)
        print(f"Average time of 30 subsequent executions (Query 2): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 2): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 2"] = (average_subsequent_time, average, margin_of_error)

        # Query 3
        start_time = time.time()
        query_result = query3(graph)  # Esecuzione della query
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)  # Conversione in JSON
            print(f"Query 3 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified ID\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)  # Tempo di esecuzione della prima query
        print(f"Response time (first execution - Query 3): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 3"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query3, percentage)
        print(f"Average time of 30 subsequent executions (Query 3): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 3): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 3"] = (average_subsequent_time, average, margin_of_error)

        # Query 4
        start_time = time.time()
        query_result = query4(graph)  # Esecuzione della query
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)  # Conversione in JSON
            print(f"Query 4 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified ID\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)  # Tempo di esecuzione della prima query
        print(f"Response time (first execution - Query 4): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 4"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query4, percentage)
        print(f"Average time of 30 subsequent executions (Query 4): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 4): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 4"] = (average_subsequent_time, average, margin_of_error)

        # Query 5
        start_time = time.time()
        query_result = query5(graph)  # Esecuzione della query
        if query_result:
            json_result = json.dumps(query_result, indent=5, default=str)  # Conversione in JSON
            print(f"Query 5 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified ID\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)  # Tempo di esecuzione della prima query
        print(f"Response time (first execution - Query 5): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 5"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query5, percentage)
        print(f"Average time of 30 subsequent executions (Query 5): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 5): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 5"] = (average_subsequent_time, average, margin_of_error)

    # Salvataggio dei tempi di risposta della prima esecuzione in un file CSV
    with open('Neo4j/ResponseTimes/neo4j_times_of_response_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Milliseconds"])
        for key, value in response_times_first_execution.items():
            percentage, query = key.split(' - ')
            writer.writerow([percentage, query, value])

    # Salvataggio dei tempi di risposta medi e intervalli di confidenza in un file CSV
    with open('Neo4j/ResponseTimes/neo4j_response_times_average_30.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Milliseconds", "Average", "Confidence Interval (Min, Max)"])
        for key, (average_time, average, margin_of_error) in average_response_times.items():
            percentage, query = key.split(' - ')
            writer.writerow([percentage, query, average_time, round(average, 2), f"[{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}]"])

if __name__ == "__main__":
    main()
