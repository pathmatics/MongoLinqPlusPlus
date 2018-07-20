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
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoLinqPlusPlus.Tests
{
    /// <summary>
    /// Test repository that connects to a mongo server running on localhost
    /// </summary>
    public class ObjectIdDocumentRepository
    {
        public static IMongoCollection<ObjectIdDocument> Collection { get; }

        static ObjectIdDocumentRepository()
        {
            MongoDefaults.MaxConnectionIdleTime = TimeSpan.FromMinutes(1);
            var client = new MongoClient("mongodb://localhost");
            var db = client.GetDatabase("mongoLinqPlusPlus");
            Collection = db.GetCollection<ObjectIdDocument>("objectIdDocs");

            // Drop any existing data and insert a new batch of documents.

            db.DropCollection("objectIdDocs");
            var docs = Enumerable.Range(0, 100)
                                 .Select(c => new ObjectIdDocument {
                                     _id = new ObjectId(new DateTime(2018, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(c), 0, 0, 0),
                                     Value = c
                                 })
                                 .ToArray();
            Collection.InsertMany(docs);


            // Now pull the documents out and put them in our TestDocuments property.  It's important that
            // we wrote them into Mongo first and them extracted them because the ObjectId _id field will
            // get set automatically by Mongo (as opposed to explicitly by us).  So now we know that our
            // in memory set of documents matches our database backed set of documents exactly.
            TestDocuments = Collection.Find(FilterDefinition<ObjectIdDocument>.Empty).ToEnumerable().ToArray();
        }

        public static IEnumerable<ObjectIdDocument> TestDocuments { get; }
        public static int Value { get; }

        /// <summary>Initialize our Mongo database with fresh data and get an IQueryable to it</summary>
        public static IQueryable<ObjectIdDocument> GetDefaultDataQueryablePlusPlus(Action<string> loggingDelegate = null, bool allowMongoDiskUse = false)
        {
            return Collection.QueryablePlusPlus(allowMongoDiskUse, loggingDelegate);
        }
    }
}