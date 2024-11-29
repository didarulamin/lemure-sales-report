const express = require('express');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');
const compression = require('compression');
const cron = require('node-cron');
const axios = require('axios');  // Ensure axios is imported
const app = express();
const port = 5000;
const path = require('path')

// Enable CORS and compression
app.use(cors());
app.use(compression());
// Serve static files from the 'dist' folder
app.use(express.static(path.join(__dirname, 'dist')));

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
    "APIKEY": APIKEY
};

// Function to connect to MongoDB
async function connectToDatabase() {
    try {
        const client = await MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true });
        db = client.db(dbName);
        collection = db.collection(COLLECTION_NAME);
        console.log('Connected to MongoDB');
    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
    }
}

// Fetch products from the API
// Fetch products from the API
async function fetchProducts() {
    let products = [];
    let currentPage = 1;
    let totalPages = 1;

    while (currentPage <= totalPages) {
        console.log(`Fetching products page ${currentPage}...`);
        try {
            const response = await axios.get(`${API_URL_PRODUCTS}?limit=${LIMIT}&page=${currentPage}`, { headers: HEADERS });
            const data = response.data.data;
            const pagination = response.data.pagination;

            products.push(...data.map(record => record.Product));

            currentPage = pagination.page + 1;
            totalPages = pagination.page_count;
        } catch (error) {
            console.error(`Error fetching products: ${error.message}`);
            break;
        }
    }

    console.log("Fetched all products.");
    return products;
}
// Fetch transactions from the API

// Fetch transactions from the API
async function fetchTransactions() {
    let transactionsByProduct = {};
    let currentPage = 1;
    let totalPages = 1;

    while (currentPage <= totalPages) {
        console.log(`Fetching transactions page ${currentPage}...`);
        try {
            const response = await axios.get(`${API_URL_TRANSACTIONS}?limit=${LIMIT}&page=${currentPage}`, { headers: HEADERS });
            const data = response.data.data;
            const pagination = response.data.pagination;

            data.forEach(record => {
                const transaction = record.StockTransaction;
                const productId = transaction.product_id;

                if (!transactionsByProduct[productId]) {
                    transactionsByProduct[productId] = [];
                }

                transactionsByProduct[productId].push(transaction);
            });

            currentPage = pagination.page + 1;
            totalPages = pagination.page_count;
        } catch (error) {
            console.error(`Error fetching transactions: ${error.message}`);
            break;
        }
    }

    console.log("Fetched all transactions.");
    return transactionsByProduct;
}

// Store or update products in MongoDB

async function storeProducts(products, transactionsByProduct) {
    const bulkOperations = [];

    products.forEach(product => {
        const productId = product.id;
        if (transactionsByProduct[productId]) {
            product.transactions = transactionsByProduct[productId];
        } else {
            product.transactions = [];
        }

        const updateOperation = {
            updateOne: {
                filter: { id: product.id },
                update: {
                    $set: product,
                },
                upsert: true,  // Insert if not found, update if found
            },
        };
        bulkOperations.push(updateOperation);
    });

    if (bulkOperations.length > 0) {
        try {
            const result = await collection.bulkWrite(bulkOperations);
            console.log(`Bulk operation result: ${result.modifiedCount} documents updated.`);
        } catch (e) {
            console.error(`Error performing bulk operation: ${e.message}`);
        }
    }

    console.log("All products with transactions stored/updated successfully.");
}


// Function to store or update the sync start timestamp
async function startSyncTimestamp() {
    const currentTimestamp = new Date();  // Get the current UTC time

    try {
        const result = await db.collection("update").updateOne(
            {},  // Empty filter to target the document (assumes only one document exists)
            {
                $set: {
                    startAt: currentTimestamp,
                },
            },
            { upsert: true }  // Create the document if it doesn't exist
        );
        console.log(`Sync start timestamp ${currentTimestamp.toISOString()} stored/updated in "update" collection`);
    } catch (error) {
        console.error('Error storing start timestamp:', error);
    }
}

// Function to store or update the sync finish timestamp
async function finishSyncTimestamp() {
    const currentTimestamp = new Date();  // Get the current UTC time

    try {
        const result = await db.collection("update").updateOne(
            {},  // Empty filter to target the document
            {
                $set: {
                    finishAt: currentTimestamp,
                },
            }
        );
        console.log(`Sync finish timestamp ${currentTimestamp.toISOString()} stored/updated in "update" collection`);
    } catch (error) {
        console.error('Error storing finish timestamp:', error);
    }
}

// Function to fetch and store data
// async function fetchAndStoreData() {
//     try {
//         const productsPromise = fetchProducts();
//         const transactionsPromise = fetchTransactions();
//         const [products, transactionsByProduct] = await Promise.all([productsPromise, transactionsPromise]);

//         await startSyncTimestamp();  // Record the start timestamp

//         await storeProducts(products, transactionsByProduct);

//         await finishSyncTimestamp();  // Record the finish timestamp
//     } catch (error) {
//         console.error('Error during data fetch and store process:', error);
//     }
// }



// Fetch a single product by ID
async function fetchProductById(productId) {
    try {
        console.log(`Fetching product ID: ${productId}`);
        const response = await axios.get(`${API_URL_PRODUCT}/${productId}.json`, { headers: HEADERS });
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
        console.log(`Fetching transactions for product ID ${productId}, page ${currentPage}`);
        try {
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

    console.log(`Fetched ${transactions.length} transactions for product ID ${productId}`);
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

// Fetch and store data for target products
async function fetchAndStoreData() {
    for (const productId of productIds) {
        try {
            const product = await fetchProductById(productId);
            if (!product) {
                console.error(`Failed to fetch product ID ${productId}`);
                continue;
            }
            await startSyncTimestamp();  // Record the start timestamp
            const transactions = await fetchTransactionsByProductId(productId);
            await storeProductWithTransactions(product, transactions);
            await finishSyncTimestamp();  // Record the finish timestamp
        } catch (error) {
            console.error(`Error processing product ID ${productId}: ${error.message}`);
        }
    }

    console.log("All selected products updated successfully");
}


// Schedule the task to run every hour
// Schedule the task to run every 5 minutes
cron.schedule('*/30 * * * *', async () => {
    console.log("Starting fetch and store task...");
    await fetchAndStoreData();
    console.log("Waiting for the next fetch cycle...");
});



// API endpoint to get products (with optional date range filter)
app.get('/api/products', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1; // Default to page 1
        const limit = parseInt(req.query.limit) || 20; // Default to 20 items per page
        const skip = (page - 1) * limit; // Calculate the number of documents to skip
        const productIds = ["1056856", "1058711"]; // Example product IDs

        // Fetch products with pagination
        const products = await db.collection("transactions")
            .find({ id: { $in: productIds } }) // Filter documents by productIds
            .skip(skip) // Skip the previous pages
            .limit(limit) // Limit to the current page size
            .toArray();

        // Calculate the total number of matching documents
        const totalDocuments = await db.collection("transactions").countDocuments({
            id: { $in: productIds },
        });

        // Calculate total pages
        const totalPages = Math.ceil(totalDocuments / limit);

        // Fetch the last update (optional)
        const lastupdate = await db.collection("update").find().toArray();

        // Send response
        res.json({
            page,
            limit,
            totalPages,
            totalDocuments,
            data: products,
            lastupdate,
        });
    } catch (err) {
        console.error('Error fetching products:', err.message);
        res.status(500).json({ message: 'Server Error' });
    }
});



app.get('/api/products/search', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1; // Default to page 1
        const limit = parseInt(req.query.limit) || 20; // Default to 20 items per page
        const skip = (page - 1) * limit; // Calculate the number of documents to skip

        // Build the query object based on request query parameters
        const query = {};

        if (req.query.id) {
            // Check if the `id` query parameter should be for 'id' or 'barcode'
            const searchValue = req.query.id;
            query.$or = [
                { id: searchValue },  // Match by product id
                { barcode: searchValue }  // Match by barcode
            ];
        }

        // Fetch products with pagination and dynamic query
        const products = await db.collection("transactions")
            .find(query) // Apply the dynamic query filter
            .skip(skip) // Skip the previous pages
            .limit(limit) // Limit to the current page size
            .toArray();

        // Get the total count of documents for calculating total pages
        const totalDocuments = await db.collection("transactions").countDocuments(query); // Count based on the query
        const totalPages = Math.ceil(totalDocuments / limit);

        // Send response
        res.json({
            page,
            limit,
            totalPages,
            totalDocuments,
            data: products,
        });
    } catch (err) {
        console.error('Error fetching products:', err.message);
        res.status(500).json({ message: 'Server Error' });
    }
});




// Catch-all route to serve index.html for any unmatched route
// app.get('*', async (req, res) => {

//     res.sendFile(path.join(__dirname, 'dist', 'index.html'));
// });

// Connect to the database and start the server
connectToDatabase().then(() => {
    app.listen(port, () => {
        console.log(`Server running on http://localhost:${port}`);
    });
});
