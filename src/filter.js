const { MongoClient } = require('mongodb');
const moment = require('moment-timezone');

const mongoURI = 'mongodb://amin:Lemure17@3.0.158.189:27017/';
const client = new MongoClient(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true });

const targetProductId = "1058711"; // Product ID to filter by

async function getRecentTransactions() {
    try {
        await client.connect();
        console.log('Connected to MongoDB');

        const db = client.db('Daftra');

        // Define the start date (5th December, 11 AM Riyadh time) and convert it to UTC
        const startDateRiyadh = moment.tz('2024-12-05 11:00:00', 'Asia/Riyadh').toDate();

        // Get the current UTC time
        const currentDateUTC = new Date();

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
                            cond: {
                                $and: [
                                    { $gte: [{ $toDate: "$$transaction.created" }, startDateRiyadh] }, // Filter by date
                                    // { $lte: [ { $toDate: "$$transaction.created" }, currentDateUTC ] },   // Ensure it's before current time
                                    { $eq: ["$$transaction.transaction_type", "2"] }                     // Filter by transaction_type = 2
                                ]
                            }
                        }
                    }
                }
            }
        ]).toArray();

        console.log('Filtered Transactions:', JSON.stringify(result[0].transactions.length, null, 2));
    } catch (err) {
        console.error('Error:', err);
    } finally {
        await client.close();
    }
}

getRecentTransactions();
