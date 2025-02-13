# Query 4: Recupera dettagli di un'azienda, i suoi amministratori, UBO e transazioni in un intervallo di date

### Descrizione
Questa query recupera i dettagli di un'azienda specifica, inclusi gli amministratori associati, i beneficiari effettivi (UBO) con partecipazione maggiore al 25%, e le transazioni effettuate in un intervallo di date specificato. L'obiettivo è confrontare le prestazioni di BaseX e Neo4j nell'esecuzione di query per il recupero di dati aziendali e delle relative entità correlate.

---

## Query

### Query BaseX
```python
def query4(session, percentage):
    company_id = 9710
    ubo_percentage = 25
    start_date = "2016-07-01"
    end_date = "2024-07-01"

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
        where xs:decimal($ubo_record/ownership_percentage) > {ubo_percentage}
        return $ubo_record

    let $transaction_ids := tokenize(substring-before(substring-after($company/transactions/text(), '['), ']'), ',\\s*')

    let $start_date := xs:date("{start_date}")
    let $end_date := xs:date("{end_date}")

    let $transactions := 
        for $transaction_id in $transaction_ids
        let $transaction_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='transactions' and id=xs:integer($transaction_id)]
        where xs:date($transaction_record/date) >= $start_date and xs:date($transaction_record/date) <= $end_date
        return $transaction_record

    let $total_transaction_amount := sum($transactions/amount)

    return 
        <result>
            {{
                $company,
                <administrators>
                    {{
                        if (exists($admins)) 
                        then $admins 
                        else <message>No administrators found</message>
                    }}
                </administrators>,
                <ubos>
                    {{
                        if (exists($ubos)) 
                        then $ubos 
                        else <message>No UBOs found with more than 25% ownership</message>
                    }}
                </ubos>,
                <transactions>
                    {{
                        if (exists($transactions)) 
                        then $transactions 
                        else <message>No transactions found in the specified period</message>
                    }}
                </transactions>,
                <total_transaction_amount>{{ $total_transaction_amount }}</total_transaction_amount>
            }}
        </result>
    """

    result = session.query(query).execute()
    return company_id, result
```

### Query Neo4j
```python
def query4(graph):
    company_id = 9710
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
```

---

# Tempi di Risposta

### Tempi di prima esecuzione

![Foto Prima Esecuzione](../Histograms/Histogram_Time_Before_Execution_Query%204.png)

### Tempi di esecuzione medi

![Foto Esecuzione Medi](../Histograms/Histogram_Average_Execution_Time_Query%204.png)