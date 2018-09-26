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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MongoLinqPlusPlus.Tests
{
    [TestClass]
    public class TimeSpanTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestRepository.GetDefaultDataQueryablePlusPlus(s => System.Diagnostics.Debug.Write(s));
        private IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        [TestMethod]
        public void DateTimeDiff()
        {
            // Use the Bson Serializer
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.Birthday - c.Birthday)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.Birthday - c.Anniversary)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.Anniversary - c.Birthday)
            )));

            // Force the Json deserializer
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new {
                    Diff = c.Birthday - c.Anniversary
                })
            )));
        }

        [TestMethod]
        public void DateTimeDiffTimespanProperties()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.Birthday - c.Birthday)
                         .Select(c => new {
                             c.Ticks,
                             c.TotalMilliseconds,
                             c.TotalSeconds,
                             c.TotalMinutes,
                             c.TotalHours,
                             c.TotalDays
                         })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.Anniversary - c.Birthday)
                         .Select(c => new {
                             c.Ticks,
                             c.TotalMilliseconds,
                             c.TotalSeconds,
                             c.TotalMinutes,
                             c.TotalHours,
                             c.TotalDays
                         })
                         .AsEnumerable()
                         .Select(c => new {
                             c.Ticks,
                             TotalMilliseconds = Math.Round(c.TotalMilliseconds, 2),
                             TotalSeconds = Math.Round(c.TotalSeconds, 2),
                             TotalMinutes = Math.Round(c.TotalMinutes, 2),
                             TotalHours = Math.Round(c.TotalHours, 2),
                             TotalDays = Math.Round(c.TotalDays, 2),
                         })
            )));
        }

        [TestMethod]
        public void DateTimePlusTimespanMath()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.Birthday + TimeSpan.FromSeconds(1))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.Birthday - TimeSpan.FromSeconds(3))
            )));
        }

        [TestMethod]
        public void TimeSpanPlusTimespanMath()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => (c.Anniversary - c.Birthday) + TimeSpan.FromSeconds(1))
            )));
        }

        // TODO: Add a proper timespan property and see what happens
        // Use math
        // Deserialize
        // Access property (totalseconds)
    }
}
