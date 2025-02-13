# Query 5: Recupera dettagli di un'azienda, i suoi amministratori, UBO, transazioni e i suoi azionisti

### Descrizione
Questa query recupera i dettagli di un'azienda specifica, compresi gli amministratori associati, i beneficiari effettivi (UBO) con una partecipazione maggiore al 25%, le transazioni effettuate in una specifica valuta e data, e i suoi azionisti. L'obiettivo è confrontare le prestazioni di BaseX e Neo4j nell'esecuzione di query per il recupero di dati aziendali e delle relative entità correlate.

---

## Query

### Query BaseX
```python
def query5(session, percentage):
    company_id = 9710
    ubo_percentage = 25
    currency = "EUR"
    date = "2003-01-01"

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

    let $date := xs:date("{date}")

    let $transactions := 
        for $transaction_id in $transaction_ids
        let $transaction_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='transactions' and id=xs:integer($transaction_id)]
        where xs:date($transaction_record/date) >= $date and xs:string($transaction_record/currency) = "{currency}"
        return $transaction_record

    let $total_transaction_amount := sum($transactions/amount)

    let $shareholder_ids := tokenize(substring-before(substring-after($company/shareholders/text(), '['), ']'), ',\\s*')

    let $shareholders := 
        for $shareholder_id in $shareholder_ids
        let $shareholder_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='shareholders' and id=xs:integer($shareholder_id)]
        return $shareholder_record

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
                    else <message>No UBOs found with more than {ubo_percentage}% ownership</message>
                }}
            </ubos>,
            <transactions>
                {{
                    if (exists($transactions)) 
                    then $transactions 
                    else <message>No transactions found in the specified period with the currency {currency}</message>
                }}
            </transactions>,
            <total_transaction_amount>{{ $total_transaction_amount }}</total_transaction_amount>,
            <shareholders>
                {{
                    if (exists($shareholders)) 
                    then $shareholders 
                    else <message>No shareholders found</message>
                }}
            </shareholders>
        }}
    </result>
    """

    result = session.query(query).execute()
    return company_id, result
```

### Query Neo4j
```python
def query5(graph):
    company_id = 9710
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
```

---

# Tempi di Risposta

### Tempi di prima esecuzione

![Foto Prima Esecuzione](../Histograms/Histogram_Time_Before_Execution_Query%205.png)

### Tempi di esecuzione medi

![Foto Esecuzione Medi](../Histograms/Histogram_Average_Execution_Time_Query%205.png)