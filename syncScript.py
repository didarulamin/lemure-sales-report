import requests
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import time

# MongoDB Connection
MONGO_URI = "mongodb://amin:Lemure17@3.0.158.189:27017/"
DATABASE_NAME = "Daftra"
COLLECTION_NAME = "transactions"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# API Configuration
API_URL_PRODUCTS = "https://amamamed.daftra.com/api2/products"
API_URL_TRANSACTIONS = "https://amamamed.daftra.com/api2/stock_transactions"
APIKEY = "70d7582b80ee4c5855daaed6872460519c0a528c"
LIMIT = 1000

HEADERS = {"APIKEY": f"{APIKEY}"}
TARGET_PRODUCT_IDS = ["1056856", "1058711"]  # Target product IDs
FETCH_INTERVAL = 1800  # 30 minutes in seconds

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
    # Fetch Products
    products = []
    current_page = 1
    total_pages = 1
    while current_page <= total_pages:
        print(f"Fetching products page {current_page}...")
        result = fetch_with_retries(f"{API_URL_PRODUCTS}?limit={LIMIT}&page={current_page}", HEADERS)
        data = result.get("data", [])
        pagination = result.get("pagination", {})
        products.extend([record["Product"] for record in data])
        current_page += 1
        total_pages = pagination.get("page_count", current_page - 1)

    # Fetch Transactions
    transactions_by_product = {}
    current_page = 1
    total_pages = 1
    while current_page <= total_pages:
        print(f"Fetching transactions page {current_page}...")
        result = fetch_with_retries(f"{API_URL_TRANSACTIONS}?limit={LIMIT}&page={current_page}", HEADERS)
        data = result.get("data", [])
        pagination = result.get("pagination", {})
        for record in data:
            transaction = record["StockTransaction"]
            product_id = transaction.get("product_id")
            if product_id not in transactions_by_product:
                transactions_by_product[product_id] = []
            transactions_by_product[product_id].append(transaction)
        current_page += 1
        total_pages = pagination.get("page_count", current_page - 1)

    # Filter and Update Products
    filtered_products = [product for product in products if str(product.get("id")) in TARGET_PRODUCT_IDS]
    for product in filtered_products:
        product_id = product.get("id")
        product["transactions"] = transactions_by_product.get(product_id, [])

    # Store in MongoDB
    operations = [
        UpdateOne({"id": product["id"]}, {"$set": product}, upsert=True)
        for product in filtered_products
    ]
    if operations:
        collection.bulk_write(operations)
        print("Bulk write operation completed successfully.")

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
