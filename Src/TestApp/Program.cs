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
using System.Collections;
using System.Diagnostics;
using System.Linq;
using MongoLinqPlusPlus.Tests;
using Newtonsoft.Json;
using UnitTests;

namespace MongoLinqPlusPlus.TestApp
{
    class Program
    {
        private static IQueryable<TestDocument> _mongoQuery = TestRepository.GetDefaultDataQueryablePlusPlus(Console.Write);
        private static IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        static void Main()
        {
            /*
            var repo = TestHelpers.InitMongoBulk(100000, out _mongoQuery, out _memryQuery, s => Console.Write(s));

            var defaultMongoQueryable = repo.Collection.AsQueryable();

            var sw = Stopwatch.StartNew();
            var docs = defaultMongoQueryable.Where(c => c.NumPets != 55).OrderBy(c => c.SSN).Take(5000).ToArray();
            Console.WriteLine("Default Mongo Queryable: " + sw.Elapsed);

            sw.Restart();
            docs = _mongoQuery.Where(c => c.NumPets != 55).OrderBy(c => c.SSN).Take(5000).ToArray();
            Console.WriteLine("LinqPlusPlus  Queryable: " + sw.Elapsed);

*/



/*            var results = _mongoQuery.Where(c => c.Birthday.Date == new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
                                     .Select(c => new {
                                         c.Birthday
                                     })
                                     .ToArray();*/

            var results = ObjectIdDocumentRepository.GetDefaultDataQueryablePlusPlus(Console.WriteLine)
                                                    .Select(c => c._id.CreationTime.Date > new DateTime(2017,2,1,0,0,0,DateTimeKind.Utc))
                                                    .Take(2)
                                                    .ToArray();
            

            /*                
    
                Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                    queryable.GroupBy(c => c.Birthday.Year)
                             .Select(c => new { First = c.First() })
                             .Take(1)
                )));
                
                Console.WriteLine("\r\n------------ TEST PROGRAM RESULTS -------------\r\n");
                var results = _mongoQuery.GroupBy(c => c.NumPets)
                             .Select(c => new { First = c.First() })
                             .Take(1)
                             .ToArray();*/

            
            var json = JsonConvert.SerializeObject(results, Formatting.Indented);
            Console.WriteLine(json);

            if (Debugger.IsAttached)
            {
                Console.WriteLine("\r\nPress any key to exit.");
                Console.ReadKey();
            }
        }
    }
}
