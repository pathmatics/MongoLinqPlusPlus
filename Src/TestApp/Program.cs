// The MIT License (MIT)
// 
// Copyright (c) 2014 Adomic, Inc
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
using System.Collections;
using System.Diagnostics;
using System.Linq;
using MongoDB.Driver.Linq;
using MongoLinqPlusPlus.Tests;
using Newtonsoft.Json;

namespace MongoLinqPlusPlus.TestApp
{
    class Program
    {
        private static IQueryable<TestDocument> _mongoQuery = TestHelpers.InitMongo(Console.Write);
        private static IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        static void Main()
        {
            var repo = TestHelpers.InitMongoBulk(100000, out _mongoQuery, out _memryQuery, s => Console.Write(s));

            var defaultMongoQueryable = repo.Collection.AsQueryable();

            var sw = Stopwatch.StartNew();
            var docs = defaultMongoQueryable.ToArray();
            Console.WriteLine("Default Mongo Queryable: " + sw.Elapsed);

            sw.Restart();
            docs = _mongoQuery.ToArray();
            Console.WriteLine("LinqPlusPlus  Queryable: " + sw.Elapsed);
            /*
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, defaultMongoQueryable }.Select(queryable =>
                queryable.ToArray()
            )));
            */
            /*
            Console.WriteLine("\r\n------------ TEST PROGRAM RESULTS -------------\r\n");
            var results = _memryQuery.Select(c => new { NumPets = 1 })
                                     .ToArray();
            
            var json = JsonConvert.SerializeObject(results, Formatting.Indented);
            Console.WriteLine(json);
            */

            if (System.Diagnostics.Debugger.IsAttached)
            {
                Console.WriteLine("\r\nPress any key to exit.");
                Console.ReadKey();
            }
        }
    }
}
