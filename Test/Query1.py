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
        '100': db['UBOs 100%'],
        '75': db['UBOs 75%'],
        '50': db['UBOs 50%'],
        '25': db['UBOs 25%']
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

def query_admins_by_company_id(company_id):
    """Trova gli amministratori associati a un'azienda specifica"""
    # Step 1: Trova i dettagli dell'azienda
    company = collections['companies']['100'].find_one({'id': company_id})
    
    if not company:
        return "Company not found."
    
    # Step 2: Ottieni la lista degli ID degli amministratori
    admin_ids_str = company.get('administrators', '[]')
    
    # Converti la stringa JSON in una lista di ID
    try:
        admin_ids = json.loads(admin_ids_str)
        if not isinstance(admin_ids, list):
            raise ValueError("Admin IDs is not a list")
        # Assicurati che tutti gli ID siano numeri
        admin_ids = [int(id) for id in admin_ids]
    except (json.JSONDecodeError, ValueError) as e:
        return f"Invalid administrator IDs format: {e}"

    # Step 3: Trova i dettagli degli amministratori
    admins = collections['administrators']['100'].find({'id': {'$in': admin_ids}})
    
    return format_as_json(admins)

# Esempio di utilizzo delle query
if __name__ == "__main__":
    # Esempio di ID dell'azienda
    company_id = 241
    print(f"Amministratori dell'azienda con ID {company_id}:\n{query_admins_by_company_id(company_id)}\n")