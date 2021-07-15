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
using System.Linq;
using MongoDB.Driver;

namespace MongoLinqPlusPlus.Tests
{
    /// <summary>
    /// Test repository that connects to a mongo server running on localhost
    /// </summary>
    public class StudentRepository
    {
        public const string COLLECTION = "students";

        public IMongoCollection<Student> Collection
        {
            get
            {
                MongoDefaults.MaxConnectionIdleTime = TimeSpan.FromMinutes(1);
                var client = new MongoClient("mongodb://localhost");
                return client.GetDatabase("mongoLinqPlusPlus").GetCollection<Student>(COLLECTION);
            }
        }

        public void DropCollection() => Collection.Database.DropCollection(COLLECTION);

        
        /// <summary>Initialize our Mongo database with fresh data and get an IQueryable to it</summary>
        public static IQueryable<Student> GetDefaultDataQueryablePlusPlus(Action<string> loggingDelegate = null, bool allowMongoDiskUse = false)
        {
            var repo = new StudentRepository();
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

        public static Student[] TestDocuments { get; } = {
            new Student {FirstName = "Tom", StudentNum = 5012050215, University = "New Orleans Clown College"},
            new Student {FirstName = "Larry", StudentNum = 0, University = "CWRU"},
            new Student {FirstName = "Tom", StudentNum = -1, University = "CWRU"},
            new Student {FirstName = "Tomas", StudentNum = 12, University = "The Mental Institute of Cleveland"},
            new Student {FirstName = "Frank", StudentNum = 89958128951892, University = "The Mental Institute of Cleveland"},
        };
    }
}