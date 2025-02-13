from os import error
import pandas as pd
from py2neo import Graph, Node, Relationship
import random


# Funzione per creare nodi e relazioni nel grafo Neo4j a partire dai dataset forniti.
def create_graph(
    graph, admins, shareholders, ubos, transactions, companies, kyc_aml_checks
):
    # Crea nodi per gli amministratori
    admin_nodes = {}
    for _, row in admins.iterrows():
        admin_node = Node(
            "Administrators",
            id=row["id"],
            name=row["name"],
            address=row["address"],
            birthdate=row["birthdate"],
            nationality=row["nationality"],
        )
        graph.create(admin_node)
        admin_nodes[row["id"]] = admin_node

    # Crea nodi per gli azionisti
    shareholder_nodes = {}
    for _, row in shareholders.iterrows():
        shareholder_node = Node(
            "Shareholders",
            id=row["id"],
            name=row["name"],
            type=row["type"],
            ownership_percentage=row["ownership_percentage"],
            address=row["address"],
            birthdate=row["birthdate"],
            nationality=row["nationality"],
        )
        graph.create(shareholder_node)
        shareholder_nodes[row["id"]] = shareholder_node

    # Crea nodi per gli UBO (Ultimate Beneficial Owners)
    ubo_nodes = {}
    for _, row in ubos.iterrows():
        ubo_node = Node(
            "Ubo",
            id=row["id"],
            name=row["name"],
            address=row["address"],
            birthdate=row["birthdate"],
            nationality=row["nationality"],
            ownership_percentage=row["ownership_percentage"],
            type=row["type"],
        )
        graph.create(ubo_node)
        ubo_nodes[row["id"]] = ubo_node

    # Crea nodi per le transazioni
    transaction_nodes = {}
    for _, row in transactions.iterrows():
        transaction_node = Node(
            "Transactions",
            id=row["id"],
            type=row["type"],
            amount=row["amount"],
            date=row["date"],
            currency=row["currency"],
        )
        graph.create(transaction_node)
        transaction_nodes[row["id"]] = transaction_node

    # Crea nodi per le aziende e le relazioni con altri nodi
    for _, row in companies.iterrows():
        company_node = Node(
            "Companies",
            id=row["id"],
            name=row["name"],
            address=row["address"],
            legal_form=row["legal_form"],
            registration_details=row["registration_details"],
            financial_data=row["financial_data"],
        )
        graph.create(company_node)

        # Crea relazioni tra aziende e amministratori
        for admin_id in eval(row["administrators"]):
            if admin_id in admin_nodes:
                rel = Relationship(
                    company_node,
                    "COMPANY_HAS_ADMINISTRATOR",
                    admin_nodes[admin_id],
                    role="Administrator",
                    start_date=random.choice(
                        ["2020-01-01", "2021-01-01", "2022-01-01"]
                    ),
                    end_date=None,
                )
                graph.create(rel)

        # Crea relazioni tra aziende e azionisti
        for shareholder_id in eval(row["shareholders"]):
            if shareholder_id in shareholder_nodes:
                rel = Relationship(
                    company_node,
                    "COMPANY_HAS_SHAREHOLDER",
                    shareholder_nodes[shareholder_id],
                    ownership_percentage=shareholder_nodes[shareholder_id][
                        "ownership_percentage"
                    ],
                    purchase_date=random.choice(
                        ["2020-01-01", "2021-01-01", "2022-01-01"]
                    ),
                )
                graph.create(rel)

        # Crea relazioni tra aziende e UBO
        for ubo_id in eval(row["ubo"]):
            if ubo_id in ubo_nodes:
                rel = Relationship(
                    company_node,
                    "COMPANY_HAS_UBO",
                    ubo_nodes[ubo_id],
                    ownership_percentage=ubo_nodes[ubo_id]["ownership_percentage"],
                    purchase_date=random.choice(
                        ["2020-01-01", "2021-01-01", "2022-01-01"]
                    ),
                )
                graph.create(rel)

        # Crea relazioni tra aziende e transazioni
        for transaction_id in eval(row["transactions"]):
            if transaction_id in transaction_nodes:
                rel = Relationship(
                    company_node,
                    "COMPANY_HAS_TRANSACTION",
                    transaction_nodes[transaction_id],
                    transaction_type=transaction_nodes[transaction_id]["type"],
                    amount=transaction_nodes[transaction_id]["amount"],
                    date=transaction_nodes[transaction_id]["date"],
                    currency=transaction_nodes[transaction_id]["currency"],
                )
                graph.create(rel)

    # Crea relazioni per i controlli KYC/AML
    for _, row in kyc_aml_checks.iterrows():
        if row["ubo_id"] in ubo_nodes:
            rel = Relationship(
                ubo_nodes[row["ubo_id"]],
                "UBO_HAS_CHECKS",
                Node(
                    "KYC_AML_Check",
                    id=row["id"],
                    check_type=row["type"],
                    result=row["result"],
                    date=row["date"],
                    notes=row["notes"],
                ),
                check_type=row["type"],
                result=row["result"],
                date=row["date"],
                notes=row["notes"],
            )
            graph.create(rel)


# Test Connessione Neo4j

print("Testando la connessione a Neo4j")

try:
    graph_test = Graph(
        "bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset100"
    )

    test_node = Node("Test", name="Connection Test")
    graph_test.create(test_node)
    print("Test passato con successo!")
except Exception as e:
    print(f"Errore nella connessione a Neo4j: {e}")
    exit(-1)


# Carica i dataset dai file CSV
admins = pd.read_csv("Dataset/File/administrators.csv", encoding="ISO-8859-1")
shareholders = pd.read_csv("Dataset/File/shareholders.csv", encoding="ISO-8859-1")
ubos = pd.read_csv("Dataset/File/ubo.csv", encoding="ISO-8859-1")
transactions = pd.read_csv("Dataset/File/transactions.csv", encoding="ISO-8859-1")
companies = pd.read_csv("Dataset/File/companies.csv", encoding="ISO-8859-1")
kyc_aml_checks = pd.read_csv("Dataset/File/kyc_aml_checks.csv", encoding="ISO-8859-1")

# Connessione ai database Neo4j
graph100 = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset100")
graph75 = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset75")
graph50 = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset50")
graph25 = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset25")

# Crea i grafi per i diversi dataset

# Il 100% del dataset completo
create_graph(
    graph100, admins, shareholders, ubos, transactions, companies, kyc_aml_checks
)

print("dataset100 caricato con successo...")

# Prende il 75% dal 100%
admins_75 = admins.sample(frac=0.75)
shareholders_75 = shareholders.sample(frac=0.75)
ubos_75 = ubos.sample(frac=0.75)
transactions_75 = transactions.sample(frac=0.75)
companies_75 = companies.sample(frac=0.75)
kyc_aml_checks_75 = kyc_aml_checks.sample(frac=0.75)
create_graph(
    graph75,
    admins_75,
    shareholders_75,
    ubos_75,
    transactions_75,
    companies_75,
    kyc_aml_checks_75,
)

print("dataset75 caricato con successo...")

# Prende il 50% dal 75%
admins_50 = admins_75.sample(frac=0.6667)  # 50% del totale = 66.67% del 75%
shareholders_50 = shareholders_75.sample(frac=0.6667)
ubos_50 = ubos_75.sample(frac=0.6667)
transactions_50 = transactions_75.sample(frac=0.6667)
companies_50 = companies_75.sample(frac=0.6667)
kyc_aml_checks_50 = kyc_aml_checks_75.sample(frac=0.6667)
create_graph(
    graph50,
    admins_50,
    shareholders_50,
    ubos_50,
    transactions_50,
    companies_50,
    kyc_aml_checks_50,
)

print("dataset50 caricato con successo...")

# Prende il 25% dal 50%
admins_25 = admins_50.sample(frac=0.5)  # 25% del totale = 50% del 50%
shareholders_25 = shareholders_50.sample(frac=0.5)
ubos_25 = ubos_50.sample(frac=0.5)
transactions_25 = transactions_50.sample(frac=0.5)
companies_25 = companies_50.sample(frac=0.5)
kyc_aml_checks_25 = kyc_aml_checks_50.sample(frac=0.5)
create_graph(
    graph25,
    admins_25,
    shareholders_25,
    ubos_25,
    transactions_25,
    companies_25,
    kyc_aml_checks_25,
)

print("dataset25 caricato con successo...")

# Stampa un messaggio di conferma
print("Data successfully loaded into Neo4j.")
