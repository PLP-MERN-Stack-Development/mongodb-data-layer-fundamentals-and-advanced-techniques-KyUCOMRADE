// queries.js

const { MongoClient } = require('mongodb');
const uri = 'mongodb+srv://plpUser:plpPassword123@plpcluster.xxxxx.mongodb.net/plp_bookstore?retryWrites=true&w=majority';

async function runQueries() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db('plp_bookstore');
    const books = db.collection('books');

    // 🧾 Task 2: Basic CRUD Operations

    // 1️⃣ Find all books in a specific genre
    const fictionBooks = await books.find({ genre: 'Fiction' }).toArray();
    console.log('Fiction Books:', fictionBooks);

    // 2️⃣ Find books published after a certain year
    const recentBooks = await books.find({ published_year: { $gt: 2000 } }).toArray();
    console.log('Books published after 2000:', recentBooks);

    // 3️⃣ Find books by a specific author
    const orwellBooks = await books.find({ author: 'George Orwell' }).toArray();
    console.log('Books by George Orwell:', orwellBooks);

    // 4️⃣ Update the price of a specific book
    await books.updateOne({ title: '1984' }, { $set: { price: 15.99 } });
    console.log('Updated price of 1984.');

    // 5️⃣ Delete a book by its title
    await books.deleteOne({ title: 'Moby Dick' });
    console.log('Deleted Moby Dick.');

    // 🧠 Task 3: Advanced Queries

    // 6️⃣ Books in stock and published after 2010
    const modernStocked = await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray();
    console.log('In-stock books published after 2010:', modernStocked);

    // 7️⃣ Projection (title, author, price)
    const projection = await books.find({}, { projection: { _id: 0, title: 1, author: 1, price: 1 } }).toArray();
    console.log('Projection (title, author, price):', projection);

    // 8️⃣ Sorting (ascending)
    const sortedAsc = await books.find().sort({ price: 1 }).toArray();
    console.log('Books sorted by price (ASC):', sortedAsc);

    // 9️⃣ Pagination (limit 5, skip 5)
    const page2 = await books.find().skip(5).limit(5).toArray();
    console.log('Page 2 Books (5 per page):', page2);

    // 🧩 Task 4: Aggregation Pipelines

    // 🔹 Average price by genre
    const avgPriceByGenre = await books.aggregate([
      { $group: { _id: '$genre', avgPrice: { $avg: '$price' } } }
    ]).toArray();
    console.log('Average price by genre:', avgPriceByGenre);

    // 🔹 Author with most books
    const topAuthor = await books.aggregate([
      { $group: { _id: '$author', totalBooks: { $sum: 1 } } },
      { $sort: { totalBooks: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('Author with most books:', topAuthor);

    // 🔹 Group by publication decade
    const byDecade = await books.aggregate([
      {
        $group: {
          _id: { $subtract: [{ $divide: ['$published_year', 10] }, { $mod: [{ $divide: ['$published_year', 10] }, 1] }] },
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log('Books grouped by decade:', byDecade);

    // ⚡ Task 5: Indexing

    await books.createIndex({ title: 1 });
    await books.createIndex({ author: 1, published_year: 1 });

    const explainResult = await books.find({ title: '1984' }).explain('executionStats');
    console.log('Explain result (performance details):', explainResult.executionStats);

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.close();
    console.log('Connection closed');
  }
}

runQueries();
