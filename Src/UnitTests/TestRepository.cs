// The MIT License (MIT)
// 
// Copyright (c) 2015 Pathmatics, Inc
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;

namespace MongoLinqPlusPlus.Tests
{
    /// <summary>
    /// Test repository that connects to a mongo server running on localhost
    /// </summary>
    public partial class TestRepository
    {
        public IMongoCollection<TestDocument> Collection
        {
            get
            {
                MongoDefaults.MaxConnectionIdleTime = TimeSpan.FromMinutes(1);
                var client = new MongoClient("mongodb://localhost");
                return client.GetDatabase("mongoLinqPlusPlus").GetCollection<TestDocument>("test2");
            }
        }

        public void DropCollection() => Collection.Database.DropCollection("test2");

        
        /// <summary>Initialize our Mongo database with fresh data and get an IQueryable to it</summary>
        public static IQueryable<TestDocument> GetDefaultDataQueryablePlusPlus(Action<string> loggingDelegate = null, bool allowMongoDiskUse = false)
        {
            var repo = new TestRepository();
            repo.DropCollection();
            repo.Collection.InsertMany(TestDocuments);
            return repo.Collection.QueryablePlusPlus(allowMongoDiskUse, loggingDelegate);
        }

        /// <summary>
        /// Initializes our test MongoDB with a set number of rows and get back Queryables to the mongo and in-memory data.
        /// </summary>
        /// <param name="numRows">The number of rows to create - there will be duplicates.</param>
        /// <param name="mongoQueryable">Out: queryable to Mongo data</param>
        /// <param name="memryQueryable">Out: queryable to in-memory data</param>
        /// <param name="loggingDelegate">The logging delegate to write log messages to</param>
        /// <param name="allowMongoDiskUse">Allow mongo disk use?</param>
        /// <returns>The test repository itself</returns>
        public static TestRepository InitMongoBulk(int numRows,
                                                   out IQueryable<TestDocument> mongoQueryable,
                                                   out IQueryable<TestDocument> memryQueryable,
                                                   Action<string> loggingDelegate = null,
                                                   bool allowMongoDiskUse = false)
        {
            var repo = new TestRepository();
            repo.DropCollection();
            memryQueryable = repo.LoadBulkTestData(numRows, loggingDelegate);
            mongoQueryable = new TestRepository().Collection.QueryablePlusPlus(allowMongoDiskUse, loggingDelegate);
            return repo;
        }

        /// <summary>
        /// Loads a large number of rows of test data into Mongo AND memory.
        /// Returns a queryable to the data in memory.  The queryable to the
        /// data in Mongo is retrievable through the MongoCollection.
        /// </summary>
        /// <param name="numDocs">Number of documents to load.</param>
        /// <param name="loggingDelegate">Logging delegate</param>
        public IQueryable<TestDocument> LoadBulkTestData(int numDocs, Action<string> loggingDelegate)
        {
            if (loggingDelegate != null)
                loggingDelegate(string.Format("Loading {0} docs into Mongo\r\n", numDocs));

            var memoryData = Enumerable.Range(0, numDocs)
                                       .Select(i => TestDocuments[i % TestDocuments.Length].CloneWithNewSSN(i))
                                       .ToArray();

            foreach (var partition in memoryData.Partition(1000))
                this.Collection.InsertMany(partition);

            if (loggingDelegate != null)
                loggingDelegate("Done bulk loading Mongo\r\n");
            return memoryData.AsQueryable();
        }
    }
}