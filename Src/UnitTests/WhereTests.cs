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

using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MongoLinqPlusPlus.Tests
{
    [TestClass]
    public class WhereTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestHelpers.InitMongo();
        private IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        [TestMethod]
        public void Where_Constant()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => true)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => false)
            )));
        }

        [TestMethod]
        public void Where_Expression()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.IsMale)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => !c.IsMale)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName == "_this_name_doesn't_exist_")
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName == "Tom" && c.LastName == "Jones")
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.CurrentAddress.State == State.CA || c.CurrentAddress.State == State.WA)
            )));
        }

        [TestMethod]
        public void Where_Operators()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.IsMale && c.NumPets > 2)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => !c.IsMale || c.NumPets >= 2)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => !(c.IsMale || c.NumPets < 2))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName != "Harry"  && !(c.NumPets <= 2))
            )));
        }

        [TestMethod]
        public void Where_Complex()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => (c.FirstName != "John" || c.FirstName == "Bob" || c.FirstName == "June")
                                && (c.IsMale || (!c.IsMale && c.NumPets >= 1))
                                && (c.CurrentAddress.State != State.WA))
            )));
        }

        [TestMethod]
        public void Where_Chained()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName == "Tom").Where(c => c.CurrentAddress.State == State.WA)
            )));
        }
    }
}
