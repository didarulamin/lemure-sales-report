import requests
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import time

# MongoDB Connection
MONGO_URI = "mongodb://amin:Lemure17@3.0.158.189:27017/"
DATABASE_NAME = "Daftra"
COLLECTION_NAME = "products"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# API Configuration
API_URL_PRODUCTS = "https://amamamed.daftra.com/api2/products"
API_URL_TRANSACTIONS = "https://amamamed.daftra.com/api2/stock_transactions"
API_URL_ORDERS_ = "https://amamamed.daftra.com/v2/api/entity/invoice/list/-1?filter[branch_id]=5&filter[id]=19170"
API_URL_ORDERS_ZID = "https://api.zid.sa/v1/managers/store/orders/{order-id}/view"
#get all transaction from all products
#from each tranction get order id and banch id
# use this info on API_URL_ORDERS to get ther invoice no
# then use invoice no on API_URL_ORDERS_ZID to get order details from that take the currency code
# store currency code in transaction in a field called tax_status. if SAR 1 else 0

APIKEY = "70d7582b80ee4c5855daaed6872460519c0a528c"
LIMIT = 1000

HEADERS = {"APIKEY": f"{APIKEY}"}
TARGET_PRODUCT_IDS = ["1056856", "1058711","1058627", "1058530"]  # Target product IDs
FETCH_INTERVAL = 1800/5  # 30 minutes in seconds

def update_sync_timestamp(sync_type):
    current_timestamp = datetime.utcnow()
    try:
        update_field = 'startAt' if sync_type == 'start' else 'finishAt'
        db['update'].update_one(
            {},
            {"$set": {update_field: current_timestamp}},
            upsert=True
        )
        print(f"Sync {sync_type} timestamp {current_timestamp.isoformat()} stored/updated")
    except Exception as error:
        print(f"Error storing {sync_type} timestamp: {error}")

def fetch_with_retries(url, headers, retries=3, backoff_factor=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor * (2 ** attempt))
            else:
                raise

def fetch_and_store_data():
    # Initialize the dictionary to hold transactions for each product
    transactions_by_product = {}

    # Fetch and store product data and transactions for each product ID in the target list
    for product_id in TARGET_PRODUCT_IDS:
        # Fetch product data
        print(f"Fetching product data for ID {product_id}...")
        product_url = f"{API_URL_PRODUCTS}/{product_id}.json"
        product_data = fetch_with_retries(product_url, HEADERS)

        if product_data and "data" in product_data:
            product = product_data["data"]["Product"]  # Accessing the "Product" dictionary

            print(f"Fetched product data: {product}")  # Debugging line to inspect the data
            
            # Initialize a list to hold all transactions for this product
            all_transactions = []

            # Fetch transaction data for this product
            print(f"Fetching transaction data for product ID {product_id}...")

            # Pagination logic: keep fetching data while "next" page exists
            page_number = 1
            while True:
                transaction_url = f"{API_URL_TRANSACTIONS}?product_id={product_id}&page={page_number}"
                transaction_data = fetch_with_retries(transaction_url, HEADERS)

                if transaction_data and "data" in transaction_data:
                    # Get the list of stock transactions for the current page
                    transactions = [record["StockTransaction"] for record in transaction_data["data"]]
                    all_transactions.extend(transactions)  # Add to the list of all transactions

                    # Check if there is another page
                    pagination = transaction_data.get("pagination", {})
                    if pagination.get("next"):
                        page_number += 1
                    else:
                        break
                else:
                    print(f"No transaction data found for product ID {product_id} on page {page_number}.")
                    break

            # Store all transactions in the product data
            product["transactions"] = all_transactions

            # Store the product with transactions in MongoDB
            operation = UpdateOne({"id": product["id"]}, {"$set": product}, upsert=True)
            collection.bulk_write([operation])  # Perform the bulk write operation for this product
            print(f"Product ID {product_id} data updated in MongoDB.")

def periodic_fetch_and_update():
    while True:
        print("Starting data fetch cycle...")
        update_sync_timestamp('start')  # Log the start timestamp
        fetch_and_store_data()  # Perform the fetch and store operation
        update_sync_timestamp('finish')  # Log the finish timestamp
        print("Waiting for the next fetch cycle...")
        time.sleep(FETCH_INTERVAL)

# Start the process
periodic_fetch_and_update()
