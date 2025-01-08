const { MongoClient } = require('mongodb');

const mongoURI = 'mongodb://amin:Lemure17@3.0.158.189:27017/';
const dbName = 'Daftra'; // Replace with your actual database name
const collectionName = 'products'; // Replace with your actual collection name

const client = new MongoClient(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true });

(async () => {
    try {
        // Connect to the MongoDB server
        await client.connect();
        console.log("Connected to MongoDB");

        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        // Aggregation pipeline
        const pipeline = [
            {
                $match: {
                    id: "1058162" // Filter for the document with the specified id
                }
            },
            {
                $unwind: "$transactions"
            },
            {
                $match: {
                    "transactions.date": { $exists: true },
                    "transactions.transaction_type": "2", // Filter for transaction_type
                    $expr: {
                        $gte: [
                            { $dateFromString: { dateString: "$transactions.date", format: "%Y-%m-%d %H:%M:%S" } },
                            new Date("2025-01-01T00:00:00Z") // Adjust date comparison
                        ]
                    }

                }
            },
            {
                $group: {
                    _id: null,
                    totalQuantity: {
                        $sum: { $abs: { $toDouble: "$transactions.quantity" } }
                    }
                }
            }
        ];

        // Execute the aggregation query
        const result = await collection.aggregate(pipeline).toArray();

        // Print the result
        console.log("Aggregation Result:", result);
    } catch (error) {
        console.error("Error:", error);
    } finally {
        // Close the client connection
        await client.close();
        console.log("Connection closed");
    }
})();
