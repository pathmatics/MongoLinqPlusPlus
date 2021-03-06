﻿// The MIT License (MIT)
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MongoLinqPlusPlus.Tests
{
    [TestClass]
    public class DateTimeTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestRepository.GetDefaultDataQueryablePlusPlus(s => System.Diagnostics.Debug.Write(s));
        private IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        [TestMethod]
        public void DateTimeSerialization()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.Birthday)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.Birthday)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.Birthday)
                         .Select(c => c.Key)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new {
                        c.FirstName,
                        c.Birthday
                    })
            )));
        }

        [TestMethod]
        public void DateTimeOperators()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new {
                             c.Birthday.Year,
                             c.Birthday.Month,
                             c.Birthday.Day,
                             c.Birthday.Hour,
                             c.Birthday.Minute,
                             c.Birthday.Second,
                             c.Birthday.Millisecond,
                             c.Birthday.DayOfWeek,
                             c.Birthday.DayOfYear,
                             c.Birthday.Date
                         })
            )));
        }

        [TestMethod]
        public void DateTimeGroupBy()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => new {
                             c.Birthday.Date
                          })
                         .Select(c => c.Key)
            )));
        }

        [TestMethod]
        public void DateTime_Range()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday == new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday > new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday >= new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday < new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday <= new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => new { c.Birthday })
            )));
        }

        [TestMethod]
        public void DateTime_Date()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday.Date == new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday.Date > new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday.Date >= new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday.Date < new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.Birthday.Date <= new DateTime(1995, 5, 24, 0, 0, 0, DateTimeKind.Utc))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.Date)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.Date.Date)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => new { c.Birthday.Date })
            )));
        }

        [TestMethod]
        public void DateTime_AddMethods()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.AddDays(0))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.AddDays(1.1))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.AddDays(-1.1))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.AddMilliseconds(10))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.AddSeconds(10))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.AddMinutes(10))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.AddHours(10))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.Add(TimeSpan.FromMinutes(2)))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => c.Birthday.Subtract(TimeSpan.FromMinutes(2)))
            )));
        }
    }
}
