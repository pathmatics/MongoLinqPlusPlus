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

using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
// ReSharper disable StringIndexOfIsCultureSpecific.1

namespace MongoLinqPlusPlus.Tests
{
    [TestClass]
    public class StringTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestRepository.GetDefaultDataQueryablePlusPlus(s => System.Diagnostics.Debug.Write(s));
        private IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        [TestMethod]
        public void String_Length()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName.Length)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length > 3)
                         .Select(c => c.FirstName)
            )));
        }

        [TestMethod]
        public void String_ToUpperLower()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.ToLower() == "tom")
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName.ToUpper())
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName.ToLower())
            )));
        }

        [TestMethod]
        public void String_StartsWith()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.StartsWith("Tom"))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new {
                             StartsWith = c.FirstName.StartsWith("Tom"),
                         })
            )));
        }

        [TestMethod]
        public void String_Contains()
        {

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new {
                             HasO = c.FirstName.Contains("o")
                         })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Contains("o"))
            )));
        }

        [TestMethod]
        public void String_IndexOf()
        {
            
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.ToLower().IndexOf("t") == 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName.IndexOf("om"))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName.IndexOf("Tom"))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName.IndexOf(""))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName.IndexOf("asdflkjadslkjfdas"))
            )));
        }

        [TestMethod]
        public void String_Concat()
        {
            
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName + c.LastName == "FrankJones")
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName + " " + c.LastName)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => "" + c.LastName)
            )));

            // Not support in Mongo 3.6.  I think we will get this in mongo 3.8.  Look at $convert.
            /*
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => 1 + c.LastName)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.LastName + 2)
            )));*/
        }

        [TestMethod]
        public void String_Substring()
        {
            
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3
                                     && c.FirstName.Substring(0, 3) == "Tom")
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => new {
                             TestA = c.FirstName.Substring(0, 3),
                             TestB = c.FirstName.Substring(0, 2),
                             TestC = c.FirstName.Substring(0, 1),
                             TestD = c.FirstName.Substring(0, 0),
                             TestE = c.FirstName.Substring(0),
                             TestF = c.FirstName.Substring(1, 2),
                             TestG = c.FirstName.Substring(1, 1),
                             TestH = c.FirstName.Substring(1, 0),
                             TestI = c.FirstName.Substring(2),
                             TestJ = c.FirstName.Substring(2, 1),
                             TestK = c.FirstName.Substring(2, 0),
                             TestL = c.FirstName.Substring(2),
                         })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(0, 3))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(0, 2))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(0, 1))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(0))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(1, 2))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(1, 1))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(1, 0))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(1))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(2, 1))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(2, 0))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName.Length >= 3)
                         .Select(c => c.FirstName.Substring(2))
            )));
        }
    }
}
