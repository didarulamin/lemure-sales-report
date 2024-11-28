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
    for (const product of products) {
        try {
            const productId = product.id;
            if (transactionsByProduct[productId]) {
                product.transactions = transactionsByProduct[productId];
            } else {
                product.transactions = [];
            }

            const existingProduct = await collection.findOne({ id: product.id });

            if (existingProduct) {
                await collection.updateOne({ id: product.id }, { $set: product });
                console.log(`Product with ID ${product.id} updated.`);
            } else {
                await collection.insertOne(product);
                console.log(`Product with ID ${product.id} added.`);
            }
        } catch (e) {
            console.error(`Error storing product data in MongoDB: ${e.message}`);
        }
    }

    console.log("All products with transactions stored/updated successfully.");
}

// Function to fetch and store data
async function fetchAndStoreData() {
    try {
        const products = await fetchProducts();
        const transactionsByProduct = await fetchTransactions();
        await storeProducts(products, transactionsByProduct);
    } catch (error) {
        console.error('Error during data fetch and store process:', error);
    }
}

// Schedule the task to run every hour
cron.schedule('0 * * * *', async () => {
    console.log('Starting fetch and store task...');
    await fetchAndStoreData();
    console.log('Waiting for the next fetch cycle...');
});

// API endpoint to get products (with optional date range filter)
app.get('/api/products', async (req, res) => {
    try {
        const { startDate, endDate } = req.query;  // Get start and end date from query params
        const matchConditions = [];

        if (startDate && endDate) {
            matchConditions.push({
                $match: {
                    "transactions.created": {
                        $gte: new Date(startDate),  // Convert startDate to Date object
                        $lte: new Date(endDate)     // Convert endDate to Date object
                    }
                }
            });
        }

        const products = await collection.aggregate([
            { $unwind: "$transactions" },  // Unwind the transactions array
            ...matchConditions,
            {
                $addFields: {
                    "transactions.created": {
                        $dateFromString: {
                            dateString: "$transactions.created",
                            format: "%Y-%m-%d %H:%M:%S",  // Match your date format
                        },
                    }
                }
            },
            {
                $group: {
                    _id: "$transactions.product_id",  // Group by product_id
                    total_quantity_in: { $sum: { $cond: [{ $eq: ["$transactions.transaction_type", "1"] }, { $abs: { $toDouble: "$transactions.quantity" } }, 0] } },
                    total_quantity_out: { $sum: { $cond: [{ $eq: ["$transactions.transaction_type", "2"] }, { $abs: { $toDouble: "$transactions.quantity" } }, 0] } },
                    total_sales_amount: { $sum: { $cond: [{ $eq: ["$transactions.transaction_type", "2"] }, { $multiply: [{ $abs: { $toDouble: "$transactions.quantity" } }, { $toDouble: "$transactions.price" }] }, 0] } },
                    sale_price: { $max: { $cond: [{ $eq: ["$transactions.transaction_type", "2"] }, { $toDouble: "$transactions.price" }, 0] } },
                    product_name: { $first: "$name" },
                    product_barcode: { $first: "$barcode" },
                    product_tax1: { $first: "$tax1" },
                    product_stock_balance: { $first: "$stock_balance" },
                    product_average_price: { $first: "$average_price" }
                }
            }
        ]).toArray();

        res.json(products);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server Error' });
    }
});


// Catch-all route to serve index.html for any unmatched route
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});
// Connect to the database and start the server
connectToDatabase().then(() => {
    app.listen(port, () => {
        console.log(`Server running on http://localhost:${port}`);
    });
});
