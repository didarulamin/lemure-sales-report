const express = require('express');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');
const compression = require('compression');
const cron = require('node-cron');
const axios = require('axios');
const path = require('path');
const app = express();
const port = 5000;

// Enable CORS and compression
app.use(cors());
app.use(compression());

// Serve static files from the 'dist' folder
app.use(express.static(path.join(__dirname, '../dist')));

// MongoDB connection configuration
const url = 'mongodb://amin:Lemure17@3.0.158.189:27017/';
const dbName = 'Daftra';
const COLLECTION_NAME = 'transactions';
let db, collection;

// API configuration
const API_URL_PRODUCTS = 'https://amamamed.daftra.com/api2/products';
const API_URL_TRANSACTIONS = 'https://amamamed.daftra.com/api2/stock_transactions';
const APIKEY = '70d7582b80ee4c5855daaed6872460519c0a528c';
const LIMIT = 1000; // Records per page
const productIds = ["1056856", "1058711"]; // Target product IDs

// Headers for API requests
const HEADERS = {
    "APIKEY": APIKEY,
};

// Function to connect to MongoDB
async function connectToDatabase() {
    try {
        const client = await MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true });
        db = client.db(dbName);
        collection = db.collection(COLLECTION_NAME);
        console.log('Connected to MongoDB');
    } catch (error) {
        console.error('Error connecting to MongoDB:', error.message);
    }
}

// Fetch a single product by ID
async function fetchProductById(productId) {
    try {
        console.log(`Fetching product ID: ${productId}`);
        const response = await axios.get(`${API_URL_PRODUCTS}/${productId}.json`, { headers: HEADERS });
        return response.data.Product;
    } catch (error) {
        console.error(`Error fetching product ID ${productId}: ${error.message}`);
        return null;
    }
}

// Fetch transactions for a specific product ID
async function fetchTransactionsByProductId(productId) {
    let transactions = [];
    let currentPage = 1;
    let totalPages = 1;

    while (currentPage <= totalPages) {
        try {
            console.log(`Fetching transactions for product ID ${productId}, page ${currentPage}`);
            const response = await axios.get(`${API_URL_TRANSACTIONS}?limit=${LIMIT}&page=${currentPage}&product_id=${productId}`, { headers: HEADERS });
            const data = response.data.data;
            const pagination = response.data.pagination;

            transactions.push(...data);

            currentPage = pagination.page + 1;
            totalPages = pagination.page_count;
        } catch (error) {
            console.error(`Error fetching transactions for product ID ${productId}: ${error.message}`);
            break;
        }
    }

    return transactions;
}

// Store or update a single product in MongoDB
async function storeProductWithTransactions(product, transactions) {
    try {
        product.transactions = transactions;

        const result = await collection.updateOne(
            { id: product.id }, // Match by product ID
            { $set: product }, // Update the product document
            { upsert: true } // Insert if not found
        );

        console.log(`Product ID ${product.id} stored/updated successfully`);
    } catch (error) {
        console.error(`Error storing product ID ${product.id}: ${error.message}`);
    }
}

// Function to store or update the sync timestamps
async function updateSyncTimestamp(type) {
    const currentTimestamp = new Date();

    try {
        const updateField = type === 'start' ? 'startAt' : 'finishAt';
        await db.collection('update').updateOne(
            {},
            { $set: { [updateField]: currentTimestamp } },
            { upsert: true }
        );
        console.log(`Sync ${type} timestamp ${currentTimestamp.toISOString()} stored/updated`);
    } catch (error) {
        console.error(`Error storing ${type} timestamp:`, error.message);
    }
}

// Fetch and store data for target products
async function fetchAndStoreData() {
    for (const productId of productIds) {
        try {
            console.log(`Processing product ID ${productId}`);
            await updateSyncTimestamp('start');
            const product = await fetchProductById(productId);
            if (!product) continue;

            const transactions = await fetchTransactionsByProductId(productId);
            await storeProductWithTransactions(product, transactions);
            await updateSyncTimestamp('finish');
        } catch (error) {
            console.error(`Error processing product ID ${productId}: ${error.message}`);
        }
    }

    console.log("All selected products updated successfully");
}

// Schedule the task to run every 30 minutes
cron.schedule('*/30 * * * *', async () => {
    console.log("Starting fetch and store task...");
    await fetchAndStoreData();
    console.log("Waiting for the next fetch cycle...");
});

// API endpoint to get products (with optional date range filter)
app.get('/api/products', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
        const skip = (page - 1) * limit;

        // Fetch products with pagination
        const products = await collection
            .find({ id: { $in: productIds } })
            .skip(skip)
            .limit(limit)
            .toArray();

        const totalDocuments = await collection.countDocuments({ id: { $in: productIds } });
        const totalPages = Math.ceil(totalDocuments / limit);
        // Fetch the last update (optional)
        const lastupdate = await db.collection("update").find().toArray();

        res.json({ page, limit, totalPages, totalDocuments, data: products, lastupdate });
    } catch (err) {
        console.error('Error fetching products:', err.message);
        res.status(500).json({ message: 'Server Error' });
    }
});

// Catch-all route to serve index.html
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '../dist', 'index.html'));
});

// Connect to the database and start the server
connectToDatabase().then(() => {
    app.listen(port, () => {
        console.log(`Server running on http://localhost:${port}`);
    });
});
