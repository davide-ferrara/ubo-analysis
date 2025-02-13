from pymongo import MongoClient
import json
from bson import ObjectId

# Connessione al server MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["UBO"]

# Definizione delle collezioni per ciascuna entitÃ  e percentuale
collections = {
    'administrators': {
        '100': db['Administrators 100%'],
        '75': db['Administrators 75%'],
        '50': db['Administrators 50%'],
        '25': db['Administrators 25%']
    },
    'shareholders': {
        '100': db['Shareholders 100%'],
        '75': db['Shareholders 75%'],
        '50': db['Shareholders 50%'],
        '25': db['Shareholders 25%']
    },
    'ubos': {
        '100': db['UBO 100%'],
        '75': db['UBO 75%'],
        '50': db['UBO 50%'],
        '25': db['UBO 25%']
    },
    'transactions': {
        '100': db['Transactions 100%'],
        '75': db['Transactions 75%'],
        '50': db['Transactions 50%'],
        '25': db['Transactions 25%']
    },
    'kyc_aml_checks': {
        '100': db['KYC_AML_Checks 100%'],
        '75': db['KYC_AML_Checks 75%'],
        '50': db['KYC_AML_Checks 50%'],
        '25': db['KYC_AML_Checks 25%']
    },
    'companies': {
        '100': db['Companies 100%'],
        '75': db['Companies 75%'],
        '50': db['Companies 50%'],
        '25': db['Companies 25%']
    }
}

def convert_objectid(obj):
    """Converte ObjectId in stringa se presente"""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: convert_objectid(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid(i) for i in obj]
    else:
        return obj

def format_as_json(cursor):
    """Formatta i documenti come JSON e li restituisce come stringa"""
    documents = list(cursor)
    documents = convert_objectid(documents)
    return json.dumps(documents, indent=4)

def query_administrators_by_id(admin_id):
    """Trova un amministratore specifico per ID"""
    results = []
    for key in collections['administrators']:
        admin = collections['administrators'][key].find_one({'id': admin_id})
        if admin:
            results.append(admin)
    return format_as_json(results) if results else "No administrator found."

def query_companies_by_legal_form(legal_form):
    """Trova tutte le aziende con un certo tipo di forma giuridica"""
    results = []
    for key in collections['companies']:
        companies = collections['companies'][key].find({'legal_form': legal_form}).limit(10)
        results.extend(companies)
    return format_as_json(results)

def query_transactions_by_currency(currency):
    """Trova tutte le transazioni di una certa valuta"""
    results = []
    for key in collections['transactions']:
        transactions = collections['transactions'][key].find({'currency': currency}).limit(10)
        results.extend(transactions)
    return format_as_json(results)

def query_ubos_with_passed_kyc():
    """Trova gli UBO che hanno passato tutti i controlli KYC/AML"""
    kyc_aml_checks = collections['kyc_aml_checks']['100']
    ubos = collections['ubos']['100']
    ubo_ids = kyc_aml_checks.distinct('ubo_id', {'result': 'Passed'})
    ubos_passed = ubos.find({'id': {'$in': ubo_ids}}).limit(10)
    return format_as_json(ubos_passed)

def query_companies_with_high_shareholders(min_percentage):
    """Trova aziende con una certa percentuale di azionisti"""
    results = []
    for key in collections['companies']:
        companies = collections['companies'][key].find({
            'shareholders': {'$elemMatch': {'ownership_percentage': {'$gte': min_percentage}}}
        }).limit(10)
        results.extend(companies)
    return format_as_json(results)

def query_sample_transactions():
    """Trova alcune transazioni per verificare i dati"""
    results = []
    for key in collections['transactions']:
        sample_transactions = collections['transactions'][key].find().limit(10)
        results.extend(sample_transactions)
    return format_as_json(results)

def query_sample_kyc_aml_checks():
    """Trova alcuni controlli KYC/AML per verificare i dati"""
    results = []
    for key in collections['kyc_aml_checks']:
        sample_checks = collections['kyc_aml_checks'][key].find().limit(10)
        results.extend(sample_checks)
    return format_as_json(results)

def query_sample_companies():
    """Trova alcune aziende per verificare i dati"""
    results = []
    for key in collections['companies']:
        sample_companies = collections['companies'][key].find().limit(10)
        results.extend(sample_companies)
    return format_as_json(results)

def query_companies_with_details():
    """Trova le aziende con i dettagli relativi ad amministratori, azionisti, UBO, transazioni e controlli KYC/AML"""
    return db.companies.aggregate([
        {
            "$lookup": {
                "from": "Administrators 100%",
                "localField": "administrators",
                "foreignField": "id",
                "as": "administrators_details"
            }
        },
        {
            "$lookup": {
                "from": "Shareholders 100%",
                "localField": "shareholders",
                "foreignField": "id",
                "as": "shareholders_details"
            }
        },
        {
            "$lookup": {
                "from": "UBO 100%",
                "localField": "ubo",
                "foreignField": "id",
                "as": "ubo_details"
            }
        },
        {
            "$lookup": {
                "from": "Transactions 100%",
                "localField": "transactions",
                "foreignField": "id",
                "as": "transactions_details"
            }
        },
        {
            "$lookup": {
                "from": "KYC_AML_Checks 100%",
                "localField": "kyc_aml_checks",
                "foreignField": "id",
                "as": "kyc_aml_checks_details"
            }
        }
    ])

def query_transactions_by_company(company_id):
    """Trova tutte le transazioni di una determinata azienda"""
    return db.companies.aggregate([
        { "$match": { "id": company_id } },
        { "$lookup": {
            "from": "Transactions 100%",
            "localField": "transactions",
            "foreignField": "id",
            "as": "transactions_details"
        }},
        { "$unwind": "$transactions_details" }
    ])

def query_ubos_with_kyc_passed():
    """Trova gli UBO che hanno passato tutti i controlli KYC/AML"""
    return db["KYC_AML_Checks 100%"].aggregate([
        { "$match": { "result": "Passed" } },
        { "$lookup": {
            "from": "UBO 100%",
            "localField": "ubo_id",
            "foreignField": "id",
            "as": "ubo_details"
        }},
        { "$unwind": "$ubo_details" }
    ])

def query_companies_with_high_shareholders(min_percentage):
    """Trova le aziende con azionisti con percentuale di possesso >= min_percentage"""
    return db.companies.aggregate([
        { "$lookup": {
            "from": "Shareholders 100%",
            "localField": "shareholders",
            "foreignField": "id",
            "as": "shareholders_details"
        }},
        { "$unwind": "$shareholders_details" },
        { "$match": { "shareholders_details.ownership_percentage": { "$gte": min_percentage } } },
        { "$group": {
            "_id": "$id",
            "company_name": { "$first": "$name" },
            "shareholders": { "$push": "$shareholders_details" }
        }}
    ])

def query_sample_data():
    """Esegue query di campioni per transazioni, controlli KYC/AML e aziende"""
    sample_transactions = db["Transactions 100%"].aggregate([{ "$sample": { "size": 10 } }])
    sample_kyc_aml_checks = db["KYC_AML_Checks 100%"].aggregate([{ "$sample": { "size": 10 } }])
    sample_companies = db.companies.aggregate([{ "$sample": { "size": 10 } }])
    
    return {
        "sample_transactions": format_as_json(sample_transactions),
        "sample_kyc_aml_checks": format_as_json(sample_kyc_aml_checks),
        "sample_companies": format_as_json(sample_companies)
    }

# Esempio di utilizzo delle funzioni
if __name__ == "__main__":
    # Query di esempio
    print("Query amministratori per ID 1:")
    print(query_administrators_by_id(1))

    print("\nQuery aziende con forma giuridica 'SRL':")
    print(query_companies_by_legal_form("SRL"))

    print("\nQuery transazioni per valuta 'USD':")
    print(query_transactions_by_currency("USD"))

    print("\nQuery UBO con controlli KYC/AML passati:")
    print(query_ubos_with_passed_kyc())

    print("\nQuery aziende con azionisti con percentuale di possesso >= 50%:")
    print(query_companies_with_high_shareholders(50))

    print("\nQuery campioni di transazioni, controlli KYC/AML e aziende:")
    print(query_sample_data())