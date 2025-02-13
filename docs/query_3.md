# Query 3: Recupera i dettagli di un'azienda, i suoi amministratori e i beneficiari effettivi

### Descrizione
Questa query recupera i dettagli di un'azienda specifica, inclusi gli amministratori associati e i beneficiari effettivi (UBO) che detengono più del 25% dell'azienda. L'obiettivo è confrontare le prestazioni di BaseX e Neo4j nell'esecuzione di query per il recupero di dati aziendali e delle relative entità correlate.

---

## Query

### Query BaseX
```python
def query3(session, percentage):
    company_id = 9710  # Id della compagnia da recuperare
    query = f"""
        declare option output:method "xml";
        declare option output:indent "yes";

        let $company := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='companies' and id={company_id}]
        
        let $admins_ids := tokenize(substring-before(substring-after($company/administrators/text(), '['), ']'), ',\\s*')
        
        let $admins := 
            for $admin_id in $admins_ids
            return collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='administrators' and id=xs:integer($admin_id)]

        let $ubo_ids := tokenize(substring-before(substring-after($company/ubo/text(), '['), ']'), ',\\s*')

        let $ubos := 
            for $ubo_id in $ubo_ids
            let $ubo_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='ubo' and id=xs:integer($ubo_id)]
            where xs:decimal($ubo_record/ownership_percentage) > 25 (: Filtro per UBO con partecipazione > 25% :)
            return $ubo_record

        return 
            <result>
                {{ 
                    $company,
                    <administrators>
                        {{ 
                            if (count($admins) > 0) 
                            then 
                                $admins 
                            else 
                                <message>No administrators found</message> 
                        }}
                    </administrators>,
                    <ubos>
                        {{ 
                            if (count($ubos) > 0) 
                            then 
                                $ubos 
                            else 
                                <message>No UBOs found with more than 25% ownership</message>
                        }}
                    </ubos>
                }}
            </result>
    """

    result = session.query(query).execute()
    return company_id, result
```

### Query Neo4j
```python
def query3(graph):
    company_id = 9710
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > 25
    RETURN c, collect(DISTINCT a) as administrators, collect(DISTINCT u) as ubos
    """
    
    result = graph.run(query).data()  # Esecuzione della query e recupero dei dati
    return result
```

---

# Tempi di Risposta

### Tempi di prima esecuzione

![Foto Prima Esecuzione](../Histograms/Histogram_Time_Before_Execution_Query%203.png)

### Tempi di esecuzione medi

![Foto Esecuzione Medi](../Histograms/Histogram_Average_Execution_Time_Query%203.png)