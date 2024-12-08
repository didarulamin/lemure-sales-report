const express = require('express');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');
const compression = require('compression');
const cron = require('node-cron');
const axios = require('axios');
const path = require('path');
const app = express();
const port = 5000;
// const moment = require('moment-timezone');

// Enable CORS and compression
app.use(cors());
app.use(compression());

// Serve static files from the 'dist' folder
app.use(express.static(path.join(__dirname, '../dist')));

// MongoDB connection configuration
const url = 'mongodb://amin:Lemure17@3.0.158.189:27017/';
const dbName = 'Daftra';
const COLLECTION_NAME = 'products';
let db, collection;

// API configuration
const API_URL_PRODUCTS = 'https://amamamed.daftra.com/api2/products';
const API_URL_TRANSACTIONS = 'https://amamamed.daftra.com/api2/stock_transactions';
const APIKEY = '70d7582b80ee4c5855daaed6872460519c0a528c';
const LIMIT = 1000; // Records per page
const productIds = ["1056856", "1058711", "1058627", "1058530"]; // Target product IDs

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

// // Fetch a single product by ID
// async function fetchProductById(productId) {
//     try {
//         console.log(`Fetching product ID: ${productId}`);
//         const response = await axios.get(`${API_URL_PRODUCTS}/${productId}.json`, { headers: HEADERS });
//         return response.data.Product;
//     } catch (error) {
//         console.error(`Error fetching product ID ${productId}: ${error.message}`);
//         return null;
//     }
// }

// // Fetch transactions for a specific product ID
// async function fetchTransactionsByProductId(productId) {
//     let transactions = [];
//     let currentPage = 1;
//     let totalPages = 1;

//     while (currentPage <= totalPages) {
//         try {
//             console.log(`Fetching transactions for product ID ${productId}, page ${currentPage}`);
//             const response = await axios.get(`${API_URL_TRANSACTIONS}?limit=${LIMIT}&page=${currentPage}&product_id=${productId}`, { headers: HEADERS });
//             const data = response.data.data;
//             const pagination = response.data.pagination;

//             transactions.push(...data);

//             currentPage = pagination.page + 1;
//             totalPages = pagination.page_count;
//         } catch (error) {
//             console.error(`Error fetching transactions for product ID ${productId}: ${error.message}`);
//             break;
//         }
//     }

//     return transactions;
// }

// // Store or update a single product in MongoDB
// async function storeProductWithTransactions(product, transactions) {
//     try {
//         product.transactions = transactions;

//         const result = await collection.updateOne(
//             { id: product.id }, // Match by product ID
//             { $set: product }, // Update the product document
//             { upsert: true } // Insert if not found
//         );

//         console.log(`Product ID ${product.id} stored/updated successfully`);
//     } catch (error) {
//         console.error(`Error storing product ID ${product.id}: ${error.message}`);
//     }
// }

// // Function to store or update the sync timestamps
// async function updateSyncTimestamp(type) {
//     const currentTimestamp = new Date();

//     try {
//         const updateField = type === 'start' ? 'startAt' : 'finishAt';
//         await db.collection('update').updateOne(
//             {},
//             { $set: { [updateField]: currentTimestamp } },
//             { upsert: true }
//         );
//         console.log(`Sync ${type} timestamp ${currentTimestamp.toISOString()} stored/updated`);
//     } catch (error) {
//         console.error(`Error storing ${type} timestamp:`, error.message);
//     }
// }

// // Fetch and store data for target products
// async function fetchAndStoreData() {
//     for (const productId of productIds) {
//         try {
//             console.log(`Processing product ID ${productId}`);
//             await updateSyncTimestamp('start');
//             const product = await fetchProductById(productId);
//             if (!product) continue;

//             const transactions = await fetchTransactionsByProductId(productId);
//             await storeProductWithTransactions(product, transactions);
//             await updateSyncTimestamp('finish');
//         } catch (error) {
//             console.error(`Error processing product ID ${productId}: ${error.message}`);
//         }
//     }

//     console.log("All selected products updated successfully");
// }

// // Schedule the task to run every 30 minutes
// (async () => {
//     console.log("Starting immediate fetch and store task...");
//     await fetchAndStoreData();
//     console.log("Initial fetch cycle complete. Waiting for the next scheduled fetch...");
// })();

// // Schedule the task to run every 30 minutes
// cron.schedule('*/30 * * * *', async () => {
//     console.log("Starting scheduled fetch and store task...");
//     await fetchAndStoreData();
//     console.log("Waiting for the next fetch cycle...");
// });

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




// app.get('/api/transactions', async (req, res) => {
//     const productIds = ["1056856", "1058711", "1058627", "1058530"];



//     try {
//         // Explicitly set the start dates
//         const startDateUTC3 = new Date(Date.UTC(2024, 10, 29, 20, 0, 0)); // 29th Nov 2024, 11 PM UTC+3 (8 PM UTC)
//         const startDateUTC3ForOthertwo = new Date(Date.UTC(2024, 11, 5, 11, 0, 0)); // 5th Dec 2024, 11 AM UTC+3 (8 AM UTC)

//         console.log('Start Date (Explicit UTC+3):', startDateUTC3);
//         console.log('Start Date (Explicit UTC+3 for 1058627 & 1058530):', startDateUTC3ForOthertwo);

//         const result = await db.collection('products').aggregate([
//             {
//                 $match: { id: { $in: productIds } }
//             },
//             {
//                 $project: {
//                     _id: 0,
//                     id: 1,
//                     transactions: {
//                         $filter: {
//                             input: "$transactions",
//                             as: "transaction",
//                             cond: {
//                                 $and: [
//                                     {
//                                         $gte: [
//                                             {
//                                                 $dateFromString: {
//                                                     dateString: "$$transaction.created",
//                                                     timezone: "Asia/Riyadh"
//                                                 }
//                                             },
//                                             {
//                                                 $cond: {
//                                                     if: { $in: ["$id", ["1058627", "1058530"]] },
//                                                     then: startDateUTC3ForOthertwo,
//                                                     else: startDateUTC3
//                                                 }
//                                             }
//                                         ]
//                                     },
//                                     {
//                                         $eq: ["$$transaction.transaction_type", "2"] // Filter transactions where transaction_type = "2"
//                                     }
//                                 ]
//                             }
//                         }
//                     }
//                 }
//             },
//             {
//                 $unwind: "$transactions"
//             },
//             {
//                 $addFields: {
//                     transactionDate: {
//                         $dateFromString: {
//                             dateString: "$transactions.created",
//                             timezone: "Asia/Riyadh"
//                         }
//                     },
//                     price: { $toDouble: "$transactions.price" }, // Convert price string to double
//                     quantity: { $abs: { $toDouble: "$transactions.quantity" } } // Convert quantity to positive double
//                 }
//             },
//             {
//                 $addFields: {
//                     totalsoldamountwithoutvat: { $multiply: ["$price", "$quantity"] } // Calculate totalsoldamountwithoutvat
//                 }
//             },
//             {
//                 $addFields: {
//                     priceWithVAT: {
//                         $cond: {
//                             if: { $eq: ["$transactions.currency_code", "SAR"] },
//                             then: { $multiply: ["$price", 1.15] }, // Add 15% VAT to price if currency is SAR
//                             else: "$price" // No VAT for other currencies
//                         }
//                     }
//                 }
//             },
//             {
//                 $addFields: {
//                     totalsoldamountwithvat: { $multiply: ["$priceWithVAT", "$quantity"] } // Calculate totalsoldamountwithvat
//                 }
//             },
//             {
//                 $addFields: {
//                     monthYear: {
//                         $dateToString: {
//                             format: "%Y-%m", // Format as "YYYY-MM" (e.g., "2024-12")
//                             date: "$transactionDate",
//                             timezone: "Asia/Riyadh"
//                         }
//                     }
//                 }
//             },
//             {
//                 $group: {
//                     _id: { monthYear: "$monthYear", productId: "$id" },

//                     monthlyTotalWithoutVAT: { $sum: "$totalsoldamountwithoutvat" },
//                     monthlyTotalWithVAT: { $sum: "$totalsoldamountwithvat" },
//                     monthlyQuantitySold: { $sum: "$quantity" }
//                 }
//             },
//             {
//                 $group: {
//                     _id: "$_id.monthYear",
//                     monthlyTotals: {
//                         $push: {
//                             productId: "$_id.productId",

//                             totalWithoutVAT: "$monthlyTotalWithoutVAT",
//                             totalWithVAT: "$monthlyTotalWithVAT",
//                             quantitySold: "$monthlyQuantitySold"
//                         }
//                     },
//                     monthlyTotalWithoutVAT: { $sum: "$monthlyTotalWithoutVAT" },
//                     monthlyTotalWithVAT: { $sum: "$monthlyTotalWithVAT" },
//                     monthlyQuantitySold: { $sum: "$monthlyQuantitySold" }
//                 }
//             },
//             {
//                 $group: {
//                     _id: null,
//                     monthlyData: {
//                         $push: {
//                             k: "$_id",
//                             v: {
//                                 details: "$monthlyTotals",
//                                 totalWithoutVAT: "$monthlyTotalWithoutVAT",
//                                 totalWithVAT: "$monthlyTotalWithVAT",
//                                 quantitySold: "$monthlyQuantitySold"
//                             }
//                         }
//                     },
//                     overallTotalWithoutVAT: { $sum: "$monthlyTotalWithoutVAT" },
//                     overallTotalWithVAT: { $sum: "$monthlyTotalWithVAT" },
//                     overallTotalQuantity: { $sum: "$monthlyQuantitySold" }
//                 }
//             },
//             {
//                 $project: {
//                     _id: 0,
//                     overallTotalWithoutVAT: 1,
//                     overallTotalWithVAT: 1,
//                     overallTotalQuantity: 1,

//                     monthlyData: { $arrayToObject: "$monthlyData" }
//                 }
//             }
//         ]).toArray();

//         console.log('Result:', result);
//         res.json(result[0]);
//     } catch (err) {
//         console.error('Error:', err);
//         res.status(500).json({ error: 'Internal Server Error' });
//     }
// });



app.get('/api/transactions', async (req, res) => {
    const productIds = ["1056856", "1058711", "1058627", "1058530"];

    try {
        // Explicitly set the start dates
        const startDateUTC3 = new Date(Date.UTC(2024, 10, 29, 20, 0, 0)); // 29th Nov 2024, 11 PM UTC+3 (8 PM UTC)
        const startDateUTC3ForOthertwo = new Date(Date.UTC(2024, 11, 5, 11, 0, 0)); // 5th Dec 2024, 11 AM UTC+3 (8 AM UTC)

        console.log('Start Date (Explicit UTC+3):', startDateUTC3);
        console.log('Start Date (Explicit UTC+3 for 1058627 & 1058530):', startDateUTC3ForOthertwo);

        const result = await db.collection('products').aggregate([
            {
                $match: { id: { $in: productIds } }
            },
            {
                $project: {
                    _id: 0,
                    id: 1,
                    name: 1,           // Include product name
                    barcode: 1,        // Include product barcode
                    transactions: {
                        $filter: {
                            input: "$transactions",
                            as: "transaction",
                            cond: {
                                $and: [
                                    {
                                        $gte: [
                                            {
                                                $dateFromString: {
                                                    dateString: "$$transaction.created",
                                                    timezone: "Asia/Riyadh"
                                                }
                                            },
                                            {
                                                $cond: {
                                                    if: { $in: ["$id", ["1058627", "1058530"]] },
                                                    then: startDateUTC3ForOthertwo,
                                                    else: startDateUTC3
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        $eq: ["$$transaction.transaction_type", "2"] // Filter transactions where transaction_type = "2"
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            {
                $unwind: "$transactions"
            },
            {
                $addFields: {
                    transactionDate: {
                        $dateFromString: {
                            dateString: "$transactions.created",
                            timezone: "Asia/Riyadh"
                        }
                    },
                    price: { $toDouble: "$transactions.price" }, // Convert price string to double
                    quantity: { $abs: { $toDouble: "$transactions.quantity" } } // Convert quantity to positive double
                }
            },
            {
                $addFields: {
                    totalsoldamountwithoutvat: { $multiply: ["$price", "$quantity"] } // Calculate totalsoldamountwithoutvat
                }
            },
            {
                $addFields: {
                    priceWithVAT: {
                        $cond: {
                            if: { $eq: ["$transactions.currency_code", "SAR"] },
                            then: { $multiply: ["$price", 1.15] }, // Add 15% VAT to price if currency is SAR
                            else: "$price" // No VAT for other currencies
                        }
                    }
                }
            },
            {
                $addFields: {
                    totalsoldamountwithvat: { $multiply: ["$priceWithVAT", "$quantity"] } // Calculate totalsoldamountwithvat
                }
            },
            {
                $addFields: {
                    monthYear: {
                        $dateToString: {
                            format: "%Y-%m", // Format as "YYYY-MM" (e.g., "2024-12")
                            date: "$transactionDate",
                            timezone: "Asia/Riyadh"
                        }
                    }
                }
            },
            {
                $group: {
                    _id: { monthYear: "$monthYear", productId: "$id" },
                    name: { $first: "$name" },            // Include product name
                    barcode: { $first: "$barcode" },      // Include product barcode
                    monthlyTotalWithoutVAT: { $sum: "$totalsoldamountwithoutvat" },
                    monthlyTotalWithVAT: { $sum: "$totalsoldamountwithvat" },
                    monthlyQuantitySold: { $sum: "$quantity" }
                }
            },
            {
                $group: {
                    _id: "$_id.monthYear",
                    monthlyTotals: {
                        $push: {
                            productId: "$_id.productId",
                            name: "$name",                 // Add name to breakdown
                            barcode: "$barcode",           // Add barcode to breakdown
                            totalWithoutVAT: "$monthlyTotalWithoutVAT",
                            totalWithVAT: "$monthlyTotalWithVAT",
                            quantitySold: "$monthlyQuantitySold"
                        }
                    },
                    monthlyTotalWithoutVAT: { $sum: "$monthlyTotalWithoutVAT" },
                    monthlyTotalWithVAT: { $sum: "$monthlyTotalWithVAT" },
                    monthlyQuantitySold: { $sum: "$monthlyQuantitySold" }
                }
            },
            {
                $group: {
                    _id: null,
                    monthlyData: {
                        $push: {
                            k: "$_id",
                            v: {
                                details: "$monthlyTotals",
                                totalWithoutVAT: "$monthlyTotalWithoutVAT",
                                totalWithVAT: "$monthlyTotalWithVAT",
                                quantitySold: "$monthlyQuantitySold"
                            }
                        }
                    },
                    overallTotalWithoutVAT: { $sum: "$monthlyTotalWithoutVAT" },
                    overallTotalWithVAT: { $sum: "$monthlyTotalWithVAT" },
                    overallTotalQuantity: { $sum: "$monthlyQuantitySold" }
                }
            },
            {
                $project: {
                    _id: 0,
                    overallTotalWithoutVAT: 1,
                    overallTotalWithVAT: 1,
                    overallTotalQuantity: 1,
                    monthlyData: { $arrayToObject: "$monthlyData" }
                }
            }
        ]).toArray();

        const lastupdate = await db.collection("update").find().toArray();
        res.json({ result: result[0], lastupdate: lastupdate });
    } catch (err) {
        console.error('Error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});





app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '../dist', 'index.html'));
});

// Connect to the database and start the server
connectToDatabase().then(() => {
    app.listen(port, () => {
        console.log(`Server running on http://localhost:${port}`);
    });
});

// pip install -r requirements.txt
