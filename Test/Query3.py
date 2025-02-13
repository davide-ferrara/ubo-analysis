from pymongo import MongoClient
import json
from bson import ObjectId

# Connessione al server MongoDB
client = MongoClient("mongodb+srv://gianni:wWp9cxpz5Ws87lGC@cluster0.jo3gan0.mongodb.net/Cluster0")
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
        '75': db['KYC_AML_Checks 100%'],
        '50': db['KYC_AML_Checks 100%'],
        '25': db['KYC_AML_Checks 100%']
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

# Esempio di utilizzo delle query
if __name__ == "__main__":
    # Query di esempio
    admin_id = 1
    print(f"Amministratore con ID {admin_id}:\n{query_administrators_by_id(admin_id)}\n")

    legal_form = 'S.p.A.'
    print(f"Aziende con forma giuridica '{legal_form}':\n{query_companies_by_legal_form(legal_form)}\n")

    currency = 'EUR'
    print(f"Transazioni con valuta '{currency}':\n{query_transactions_by_currency(currency)}\n")

    print(f"UBO che hanno passato tutti i controlli KYC/AML:\n{query_ubos_with_passed_kyc()}\n")

    min_percentage = 10.0
    print(f"Aziende con azionisti con percentuale di possesso >= {min_percentage}%:\n{query_companies_with_high_shareholders(min_percentage)}\n")

    # Verifica dei dati
    print(f"Campione di transazioni:\n{query_sample_transactions()}\n")
    print(f"Campione di controlli KYC/AML:\n{query_sample_kyc_aml_checks()}\n")
    print(f"Campione di aziende:\n{query_sample_companies()}\n")