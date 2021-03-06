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

using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MongoLinqPlusPlus.Tests
{
    [TestClass]
    public class GroupByTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestRepository.GetDefaultDataQueryablePlusPlus(s => System.Diagnostics.Debug.Write(s));
        private IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        [TestMethod]
        public void GroupBy()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => c.Key)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.IsMale)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.SSN)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => new {
                             Name = c.FirstName,
                             c.SSN
                         })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => new {
                             Name = c.FirstName,
                             c.SSN
                         })
                         .Select(c => c.Key)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => new {
                             Name = c.FirstName,
                             c.SSN
                         })
                         .Select(c => c.Key.SSN)
            )));
        }

        [TestMethod]
        public void GroupByExpression()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.Birthday.Date)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.Birthday.Year + 1)
            )));
        }

        [TestMethod]
        public void GroupByNewExpression()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => new { c.NumPets })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => new { Bar = c.NumPets + 2 })
            )));
        }

        [TestMethod]
        public void GroupByParameterExpression()
        {
            // TODO: Support this
            /*
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c)
            )));*/

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => c.Key)
                         .GroupBy(c => c)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => c.Key)
                         .GroupBy(c => c)
                         .Select (c => c.Key)
            )));
        }

        [TestMethod]
        public void GroupBy_Aggregated()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName).Where(c => c.Count() > 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName).Where(c => c.Sum(d => d.NumPets) > 2)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName).Count(c => c.Count() > 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName).Select(c => c.Max(d => d.NumPets))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName).Select(c => c.Max(d => d.NumPets))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null)
                         .GroupBy(c => c.FirstName)
                         .Select(c => c.Average(d => d.OldIds.Count()))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null)
                    .GroupBy(c => c.FirstName)
                    .Select(c => c.Sum(d => new[] { true }.Contains(d.IsMale) ? 1 : 0))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null)
                    .GroupBy(c => c.FirstName)
                    .Select(c => c.Count(d => new[] { true }.Contains(d.IsMale)))
            )));
        }

        [TestMethod]
        public void GroupBySubSelect()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.LastName)
                         .OrderBy(c => c.Key)
                         .Select(c => c.Select(x => x.NumPets))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.LastName)
                         .Select(c => c.Select(x => x.NumPets))
                         .SelectMany(c => c)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.LastName)
                         .Select(c => new {
                             LastName = c.Key,
                             Pets = c.Select(x => x.NumPets)
                         })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.LastName)
                         .Select(c => new {
                             LastName = c.Key,
                             Pets = c.Select(x => x.NumPets)
                         })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.LastName)
                         .Select(c => new {
                             LastName = c.Key,
                             Pets = c.Select(x => x.NumPets)
                         })
                         .SelectMany(c => c.Pets)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.LastName)
                         .Select(c => new {
                             LastName = c.Key,
                             Pets = c.Select(x => new {
                                 x.FirstName,
                                 x.NumPets
                             })
                         })
                         .Where(c => c.Pets.Count() > 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.LastName)
                         .Select(c => new {
                             LastName = c.Key,
                             Pets = c.Select(x => x.NumPets)
                         })
                         .Select(c => new {
                             c.LastName,
                             NumPets = c.Pets.Count()
                         })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new { c.LastName, c.FirstName, c.NumPets })
                         .GroupBy(c => c.LastName)
                         .Select(c => new {
                             LastName = c.Key,
                             Pets = c.Select(x => new {
                                 x.FirstName,
                                 x.NumPets
                             })
                         })
            )));
        }
    }
}
