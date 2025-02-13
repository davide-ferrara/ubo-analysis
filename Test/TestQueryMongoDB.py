from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Use your MongoDB URI if hosted remotely
db = client['UBO']  # Replace with your database name

# Collections
administrators_collection = db['Administrators 100%']
shareholders_collection = db['Shareholders 100%']
ubo_collection = db['UBO 100%']
transactions_collection = db['Transactions 100%']
kyc_aml_checks_collection = db['KYC_AML_Checks 100%']
companies_collection = db['Companies 100%']

# Query 1: Retrieve Detailed Information About Each Company
def query_company_details():
    return companies_collection.aggregate([
        # Join with administrators
        {
            '$lookup': {
                'from': 'Administrators 100%',
                'localField': 'administrators',
                'foreignField': 'id',
                'as': 'administrator_details'
            }
        },
        # Join with shareholders
        {
            '$lookup': {
                'from': 'Shareholders 100%',
                'localField': 'shareholders',
                'foreignField': 'id',
                'as': 'shareholder_details'
            }
        },
        # Join with UBOs
        {
            '$lookup': {
                'from': 'UBO 100%',
                'localField': 'ubo',
                'foreignField': 'id',
                'as': 'ubo_details'
            }
        },
        # Join with transactions
        {
            '$lookup': {
                'from': 'Transactions 100%',
                'localField': 'transactions',
                'foreignField': 'id',
                'as': 'transaction_details'
            }
        },
        # Join with KYC/AML checks
        {
            '$lookup': {
                'from': 'KYC_AML_Checks 100%',
                'localField': 'kyc_aml_checks',
                'foreignField': 'id',
                'as': 'kyc_aml_check_details'
            }
        },
        # Project the necessary fields
        {
            '$project': {
                'name': 1,
                'address': 1,
                'legal_form': 1,
                'administrator_details.name': 1,
                'shareholder_details.name': 1,
                'ubo_details.name': 1,
                'ubo_details.ownership_percentage': 1,
                'transaction_details': 1,
                'kyc_aml_check_details': 1
            }
        }
    ])

# Query 2: Find Companies with High-Risk UBOs Based on Failed KYC/AML Checks
def query_high_risk_companies():
    return companies_collection.aggregate([ 
        # Join with UBOs
        {
            '$lookup': {
                'from': 'UBO 100%',
                'localField': 'ubo',
                'foreignField': 'id',
                'as': 'ubo_details'
            }
        },
        # Unwind the UBO details to process each UBO individually
        {
            '$unwind': '$ubo_details'
        },
        # Join with KYC/AML checks
        {
            '$lookup': {
                'from': 'KYC_AML_Checks 100%',
                'localField': 'ubo_details.id',
                'foreignField': 'ubo_id',
                'as': 'kyc_aml_check_details'
            }
        },
        # Unwind the KYC/AML check details
        {
            '$unwind': '$kyc_aml_check_details'
        },
        # Match failed checks
        {
            '$match': {
                'kyc_aml_check_details.result': 'Failed'
            }
        },
        # Project the necessary fields
        {
            '$project': {
                'company_name': '$name',
                'ubo_name': '$ubo_details.name',
                'ubo_ownership_percentage': '$ubo_details.ownership_percentage',
                'failed_check_type': '$kyc_aml_check_details.type',
                'failed_check_date': '$kyc_aml_check_details.date'
            }
        }
    ])

# Query 3: Find Companies with UBO Ownership Above 25%
def query_ubo_above_25():
    return companies_collection.aggregate([
        # Join with UBOs
        {
            '$lookup': {
                'from': 'UBO 100%',
                'localField': 'ubo',
                'foreignField': 'id',
                'as': 'ubo_details'
            }
        },
        # Unwind the UBO details
        {
            '$unwind': '$ubo_details'
        },
        # Match UBOs with ownership above 25%
        {
            '$match': {
                'ubo_details.ownership_percentage': {'$gt': 25}
            }
        },
        # Project the necessary fields
        {
            '$project': {
                'name': 1,
                'ubo_details.name': 1,
                'ubo_details.ownership_percentage': 1
            }
        }
    ])

# Query 4: Analyze Transactions for a Specific UBO
def query_transactions_for_ubo(ubo_id):
    return companies_collection.aggregate([
        # Match companies with the specific UBO
        {
            '$match': {
                'ubo': ubo_id
            }
        },
        # Join with transactions
        {
            '$lookup': {
                'from': 'Transactions 100%',
                'localField': 'transactions',
                'foreignField': 'id',
                'as': 'transaction_details'
            }
        },
        # Project the necessary fields
        {
            '$project': {
                'name': 1,
                'transaction_details': 1
            }
        }
    ])

# Query 5: Perform AML/KYC Checks on UBOs
def query_failed_kyc_aml():
    return kyc_aml_checks_collection.aggregate([
        # Match failed checks
        {
            '$match': {
                'result': 'Failed'
            }
        },
        # Join with UBOs
        {
            '$lookup': {
                'from': 'UBO 100%',
                'localField': 'ubo_id',
                'foreignField': 'id',
                'as': 'ubo_details'
            }
        },
        # Unwind the UBO details
        {
            '$unwind': '$ubo_details'
        },
        # Project the necessary fields
        {
            '$project': {
                'ubo_details.name': 1,
                'ubo_details.address': 1,
                'ubo_details.nationality': 1,
                'type': 1,
                'date': 1,
                'notes': 1
            }
        }
    ])

# Execute queries and print results
if __name__ == "__main__":
    print("Company Details:")
    for company in query_company_details():
        print(company)
    
    print("\nHigh Risk Companies:")
    for high_risk_company in query_high_risk_companies():
        print(high_risk_company)

    print("\nCompanies with UBO Ownership Above 25%:")
    for company in query_ubo_above_25():
        print(company)

    ubo_id = 1  # Replace with the UBO id you want to analyze
    print(f"\nTransactions for UBO ID {ubo_id}:")
    for company in query_transactions_for_ubo(ubo_id):
        print(company)

    print("\nUBOs with Failed KYC/AML Checks:")
    for check in query_failed_kyc_aml():
        print(check)

# Close the MongoDB connection
client.close()
