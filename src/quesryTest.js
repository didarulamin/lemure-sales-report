const { MongoClient } = require('mongodb');

const mongoURI = 'mongodb://amin:Lemure17@3.0.158.189:27017/';
const client = new MongoClient(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true });

const targetProductId = "1058627"; // Product ID to filter by
// const targetProductId = "1058530"; // Product ID to filter by
// const targetProductId = "1056856"; // Product ID to filter by
// const targetProductId = "1058711"; // Product ID to filter by
const transactionId = "22838";     // Transaction ID to filter by

async function getTransactionById() {
    try {
        await client.connect();
        console.log('Connected to MongoDB');

        const db = client.db('Daftra');

        const result = await db.collection('products').aggregate([
            {
                $match: { id: targetProductId }
            },
            {
                $project: {
                    _id: 0,
                    transactions: {
                        $filter: {
                            input: "$transactions",
                            as: "transaction",
                            cond: { $eq: ["$$transaction.id", transactionId] }
                        }
                    }
                }
            }
        ]).toArray();

        if (result.length > 0 && result[0].transactions.length > 0) {
            console.log('Filtered Transaction:', JSON.stringify(result[0].transactions, null, 2));
        } else {
            console.log('No matching transaction found.');
        }
    } catch (err) {
        console.error('Error:', err);
    } finally {
        await client.close();
    }
}

getTransactionById();
