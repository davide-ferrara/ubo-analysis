# Query 2: Recupera i dettagli di un'azienda e i suoi amministratori

### Descrizione
Questa query recupera i dettagli di un'azienda specifica dal database, inclusi gli amministratori associati all'azienda. L'obiettivo è confrontare le prestazioni di BaseX e Neo4j nell'esecuzione di query per il recupero di dati aziendali e delle relative entità correlate.

---

## Query

### Query BaseX
```python
def query2(session, percentage):
    company_id = 9710  # L'ID dell'azienda di cui vuoi recuperare i dettagli
    
    query = f"""
        declare option output:method "xml";
        declare option output:indent "yes";

        let $company := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='companies' and id=9133]

        let $admins_ids := tokenize(substring-before(substring-after($company/administrators/text(), '['), ']'), ',\\s*')

        let $admins := 
            for $admin_id in $admins_ids
            return collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='administrators' and id=xs:integer($admin_id)]

        return 
            <result>
                {{ 
                    $company,
                    $admins
                }}
             </result>
    """
    
    result = session.query(query).execute()
    return company_id, result
```

### Query Neo4j
```python
def query2(graph):
    company_id = 9710
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    RETURN c, collect(a) as administrators
    """
    result = graph.run(query).data()  # Esecuzione della query e recupero dei dati
    return result
```

---

# Tempi di Risposta

### Tempi di prima esecuzione

![Foto Prima Esecuzione](../Histograms/Histogram_Time_Before_Execution_Query%202.png)

### Tempi di esecuzione medi

![Foto Esecuzione Medi](../Histograms/Histogram_Average_Execution_Time_Query%202.png)