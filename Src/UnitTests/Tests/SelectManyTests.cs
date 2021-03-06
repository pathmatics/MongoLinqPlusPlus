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
    public class SelectManyTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestRepository.GetDefaultDataQueryablePlusPlus(s => System.Diagnostics.Debug.Write(s));
        private IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        [TestMethod]
        public void SelectMany_ArrayProperty()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.SelectMany(c => c.PreviousAddresses)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null)
                         .SelectMany(c => c.OldIds)
            )));
        }

        [TestMethod]
        public void SelectMany_DocumentIsArray()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.PreviousAddresses)
                         .SelectMany(c => c)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.PreviousAddresses)
                         .SelectMany(c => c)
                         .Select(c => c.Zip)
                         .OrderBy(c => c)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null)
                         .Select(c => c.OldIds)
                         .SelectMany(c => c)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.OldIds)
                         .Where(c => c != null)
                         .SelectMany(c => c)
            )));
        }

        [TestMethod]
        public void SelectMany_ThenSelect()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.SelectMany(c => c.PreviousAddresses)
                         .Select(c => c.Zip)
            )));

            // Try the State property which has a field name mapping
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.SelectMany(c => c.PreviousAddresses)
                         .Select(c => c.State)
            )));

            // Try the State property which has a field name mapping
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.SelectMany(c => c.PreviousAddresses)
                         .Select(c => new {
                               c.State,
                               c.Zip
                         })
            )));

            // Make sure we can do something useful with the result
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.SelectMany(c => c.PreviousAddresses)
                         .Select(c => new {
                             c.State,
                             c.Zip
                         })
                         .GroupBy(c => c.Zip)
                         .Select(c => new {
                             State = c.Key,
                             Count = c.Count()
                         })
            )));
        }

        [TestMethod]
        public void SelectMany_SubSelect()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.SelectMany(c => c.PreviousAddresses.Select(d => new {
                             d.Zip
                         }))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.SelectMany(c => c.PreviousAddresses.Select(d => d.Zip))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.SelectMany(c => c.PreviousAddresses.Select(d => new {
                    c.FirstName,
                    d.Zip
                }))
            )));
        }
    }
}
