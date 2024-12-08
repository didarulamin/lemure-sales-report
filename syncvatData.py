#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pymongo import MongoClient
from datetime import datetime, timezone
import requests
# import schedule
import time

# MongoDB connection details
DATABASE_NAME = "Daftra"
COLLECTION_NAME = "products"

# MongoDB client
client = MongoClient('mongodb://amin:Lemure17@3.0.158.189:27017/')
db = client[DATABASE_NAME]
products_collection = db[COLLECTION_NAME]

# API URLs and tokens
API_URL_ORDERS = "https://amamamed.daftra.com/v2/api/entity/invoice/list/-1"
API_URL_ORDERS_ZID = "https://api.zid.sa/v1/managers/store/orders/{order_id}/view"
APIKEY = "70d7582b80ee4c5855daaed6872460519c0a528c"
AUTH_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIzODQxIiwianRpIjoiY2ZkZGQ2NTA5NDQ4MTlkMWM1MDJjNzEwMmM2NjViYjdlYmMwNDU4NDFhZDlhYzlkMjczODQ0YTM3YTQxYjgxZTA3MzI0MjQ0OGIwNTc2OTkiLCJpYXQiOjE3Mjk0MTM0MjMuNzkxODM3LCJuYmYiOjE3Mjk0MTM0MjMuNzkxODQsImV4cCI6MTc2MDk0OTQyMy43MzY5ODgsInN1YiI6IjQ4MzU5NSIsInNjb3BlcyI6WyJ0aGlyZF9hY2NvdW50X3JlYWQiLCJ0aGlyZF92YXRfcmVhZCIsInRoaXJkX2NhdGVnb3JpZXNfcmVhZCIsInRoaXJkX2NhdGVnb3JpZXNfd3JpdGUiLCJ0aGlyZF9jdXN0b21lcnNfcmVhZCIsInRoaXJkX2N1c3RvbWVyc193cml0ZSIsInRoaXJkX29yZGVyX3JlYWQiLCJ0aGlyZF9vcmRlcl93cml0ZSIsInRoaXJkX2NvdXBvbnNfd3JpdGUiLCJ0aGlyZF9kZWxpdmVyeV9vcHRpb25zX3JlYWQiLCJ0aGlyZF9kZWxpdmVyeV9vcHRpb25zX3dyaXRlIiwidGhpcmRfYWJhbmRvbmVkX2NhcnRzX3JlYWQiLCJ0aGlyZF9wYXltZW50X3JlYWQiLCJ0aGlyZF93ZWJob29rX3JlYWQiLCJ0aGlyZF93ZWJob29rX3dyaXRlIiwidGhpcmRfcHJvZHVjdF9yZWFkIiwidGhpcmRfcHJvZHVjdF93cml0ZSIsInRoaXJkX2NvdW50cmllc19yZWFkIiwidGhpcmRfY2F0YWxvZ193cml0ZSIsInRoaXJkX2ludmVudG9yeV9yZWFkIiwidGhpcmRfanNfd3JpdGUiLCJ0aGlyZF9idW5kbGVfb2ZmZXJzX3JlYWQiLCJ0aGlyZF9jcmVhdGVfb3JkZXIiLCJ0aGlyZF9wcm9kdWN0X3N0b2NrX3JlYWQiLCJ0aGlyZF9wcm9kdWN0X3N0b2NrX3dyaXRlIiwidGhpcmRfaW52ZW50b3J5X3dyaXRlIiwiZW1iZWRkZWRfYXBwc190b2tlbnNfd3JpdGUiLCJ0aGlyZF9sb3lhbHR5X3JlYWQiLCJ0aGlyZF9sb3lhbHR5X3dyaXRlIiwidGhpcmRfb3JkZXJfcmV2ZXJzZV93cml0ZSIsInRoaXJkX29yZGVyX3JldmVyc2VfcmVhZCJdfQ.r95wsqG9nRBKS-ucOYPwHtAM0rX63gfA982TjsB4pimL1k9j46AcUHo3cfNx0hQRcbiMc1x_YwKnSt4C9siKcaLc7rLu2e2Rl7QB5Sn3kAmxt-7pWDW0kdAPG4zYlUWNuonVZjTBNtupSMR9LMJYpYOx7J_5J6dyApgaI72no5v1OLomQhWfSAKJu4Qgrdc7N19_AqRBTXJxvrHq1UfUxmzt84PBopPJt0RICDUajxWE8DT59RQmqjN-gHtXgObRhWf7q4xaeAMJML6XWBzERdtC5OQ6lHrnCeP22x08nG3kbd7YrPLifVopq5f7S6vn-7KDbaGixc20s7BcLBQWO1BAtMR5nyt4caBDZpHRHcvEb7nvjBec6IjYdXIT5zgqppONswnmjArBtzKrT27gIB-3NlLDpi9i1EMATGRMzDyh6lNpNF2LBAhUBp6PtVRptOpMN1umzh3ZSXudOsv-IfLpbA7jj5-T58bSjMwgvfuqNsjtiITRjrskoNMGo6EYEbguZR0KC8kjIsq4alwfBGLosXzQPwn1bTeNfaMMiVxftJ8tcOzNFJ7ga00OzYF4HfBbA-SZtoWO7OyYn_1p1xiNZohpXSgSdvLUQ1Bvnfxj6lDng6qJuHsYGybe4PjH3FKMNnZJF7_lZ4FH87Mmc4XFkDj8uCfXaiMz1tCpLq4"
MANAGER_TOKEN = "eyJpdiI6InlhY25Nb0N6RWd2SFJ3MlkyTkV5Rmc9PSIsInZhbHVlIjoiQlJBSlVzSFFKRVI2Mm95ZDlicHpDQjBwMk0wa3VzbkZFNFZMRXhmNHE1TUdaSlo0OWRTdU5qRUtJSU5RZHhIMVIwQTRSNlFQUE9odG9zdDJ0c2ZYTGlKVFdzci9kZlhWSktCQmVhOS9HVFNVZjVrMHhob255aEszYXRhMEpRMlByMTFBOWVCblRGb1NTZWcxWWJLOHZIK0NIUERSSHhhbjNac3VSUDl2M3JMbmh1U2paWmJqMUpLRURLbWdUOGVKRG0vTlRGOUdFbHBZQzkveXlVREVKNHhBZGdyeXFrQnByS1J4S2NlYlpaST0iLCJtYWMiOiJjNDY4Y2QyNzc5MzBhZjhmOTRjYTAyMWI4MGNiYmUxOTUwMDE2ZjM4NThiOGVjMWVhMjY5NTg4NGEyMTIwZDVkIiwidGFnIjoiIn0="

# Headers
daftra_headers = {
    "APIKEY": APIKEY,
    "Content-Type": "application/json"
}

zid_headers = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "X-Manager-Token": MANAGER_TOKEN,
    "Accept": "application/json",
}

# Start dates
start_date_utc3 = datetime(2024, 11, 29, 20, 0, 0, tzinfo=timezone.utc)
start_date_utc3_for_others = datetime(2024, 12, 5, 11, 0, 0, tzinfo=timezone.utc)

product_ids = ["1056856", "1058711", "1058627", "1058530"]

# Aggregation pipeline
def get_filtered_results():
    pipeline = [
        {
            "$match": {
                "id": {"$in": product_ids}
            }
        },
        {
            "$project": {
                "_id": 1,
                "id": 1,
                "name": 1,
                "barcode": 1,
                "transactions": {
                    "$filter": {
                        "input": "$transactions",
                        "as": "transaction",
                        "cond": {
                            "$and": [
                                {
                                    "$gte": [
                                        {
                                            "$dateFromString": {
                                                "dateString": "$$transaction.created",
                                                "timezone": "Asia/Riyadh"
                                            }
                                        },
                                        {
                                            "$cond": {
                                                "if": {"$in": ["$id", ["1058627", "1058530"]]},
                                                "then": start_date_utc3_for_others,
                                                "else": start_date_utc3
                                            }
                                        }
                                    ]
                                },
                                {
                                    "$eq": ["$$transaction.transaction_type", "2"]
                                }
                            ]
                        }
                    }
                }
            }
        },
    ]
    return list(products_collection.aggregate(pipeline))

# Function to fetch currency code from the APIs
def fetch_currency_code(order_id, branch_id):
    try:
        daftra_request_url = f"{API_URL_ORDERS}?filter[branch_id]={branch_id}&filter[id]={order_id}"
        daftra_response = requests.get(daftra_request_url, headers=daftra_headers)
        daftra_response.raise_for_status()

        data = daftra_response.json().get("data", [])
        if not data:
            return None

        no_field = data[0].get("no")
        if not no_field:
            return None

        zid_request_url = API_URL_ORDERS_ZID.format(order_id=no_field)
        zid_response = requests.get(zid_request_url, headers=zid_headers)
        zid_response.raise_for_status()

        return zid_response.json().get("order", {}).get("currency_code")

    except requests.RequestException as e:
        print(f"Error fetching currency code for Order ID {order_id}: {e}")
        return None

# Main function to update transactions
def update_transactions():
    filtered_results = get_filtered_results()
    

    for product in filtered_results:
        updated_transactions = []
        transactions = product.get("transactions", [])

        for transaction in transactions:
            order_id = transaction.get("order_id")
            print(order_id)
            branch_id = transaction.get("branch_id")
            transaction_id = transaction.get("id")

            if not order_id or not branch_id:
                print(f"Skipping transaction {transaction_id}: Missing order_id or branch_id.")
                continue

            currency_code = fetch_currency_code(order_id, branch_id)
            print(currency_code)

            if currency_code:
                transaction["currency_code"] = currency_code

            updated_transactions.append(transaction)
            

        if updated_transactions:
            result = products_collection.update_one(
                {"_id": product["_id"]},
                {"$set": {"transactions": updated_transactions}}
            )
            if result.modified_count:
                print(f"Product {product['_id']} updated successfully.")
            else:
                print(f"No updates made for Product {product['_id']}.")

# Schedule the task to run every hour
# schedule.every(1).hour.do(update_transactions)

# Run the scheduler
if __name__ == "__main__":
    print("Starting scheduled task...")
    update_transactions()
  


# In[ ]:





# In[ ]:




