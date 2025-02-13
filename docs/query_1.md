# Query 1: Recupera un'azienda per nome

### Descrizione
Questa query recupera un'azienda specifica dal database utilizzando il nome dell'azienda. L'obiettivo è confrontare le prestazioni di BaseX e Neo4j nell'esecuzione di query per il recupero di dati aziendali.

---

## Query

### Query BaseX
```python
def query1(session, percentage, entity_type="companies"):
    company_name = 'Kelly-Decker'
    
    # Query dinamica che cambia in base al tipo di entità
    query = f"""
        for $c in collection(concat("UBO_",'{percentage}'))//ubo_record[@entity_type='{entity_type}']
        where $c/name = '{company_name}'
        return $c
    """
    
    query_obj = session.query(query)
    result = query_obj.execute()
    
    return company_name, result
```

### Query Neo4j
```python
def query1(graph):
    company_name = 'Kelly-Decker'
    query = f"""
    MATCH (c:Companies {{name: '{company_name}'}})
    RETURN c
    """
    result = graph.run(query).data()  # Esecuzione della query e recupero dei dati
    return result
```

---

# Tempi di Risposta

### Tempi di prima esecuzione

![Foto Prima Esecuzione](../Histograms/Histogram_Time_Before_Execution_Query%201.png)

### Tempi di esecuzione medi

![Foto Esecuzione Medi](../Histograms/Histogram_Average_Execution_Time_Query%201.png)