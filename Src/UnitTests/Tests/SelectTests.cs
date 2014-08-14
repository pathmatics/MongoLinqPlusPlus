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
using UnitTests;

namespace MongoLinqPlusPlus.Tests
{
    [TestClass]
    public class SelectTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestHelpers.InitMongo(s => System.Diagnostics.Debug.Write(s));
        private IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();

        [TestMethod]
        public void Select_Constant()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => 0)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => 15)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => true)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => "tom")
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => "tom")
            )));

            /* Is it worth making this work?  new { NumPets = "Tom" } is getting simplified into a constant
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new { NumPets = "Tom"})
            )));
             */

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new {
                    c.FirstName,
                    NumPets = 1
                })
            )));
        }

        [TestMethod]
        public void Select_Expression()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.IsMale)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.FirstName)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.GPA)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.NumPets * 2 - 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => new {
                    c.IsMale,
                    IsFemale = !c.IsMale,
                    NumPets = c.NumPets * 15 - 2,
                    NumPetsAgain = c.NumPets * 15 - 2,
                    State = c.CurrentAddress.State,
                    IsInCali = c.CurrentAddress.State == States.CA,
                    c.SSN
                })
            )));
        }

        [TestMethod]
        public void Select_Method()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => new
                         {
                             FirstName = c.Key,
                             Sum = c.Sum(d => d.NumPets),
                             Count = c.Count(),
                             Average = c.Average(d => d.NumPets),
                             Min = c.Min(d => d.NumPets),
                             Max = c.Max(d => d.NumPets)
                         })
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => c.Sum(d => d.NumPets))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => c.Average(d => d.NumPets))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => c.Min(d => d.NumPets))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => c.Max(d => d.NumPets))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.GroupBy(c => c.FirstName)
                         .Select(c => c.Count())
            )));
        }

        [TestMethod]
        public void Select_Operators()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => (c.GPA * 2 + 4) / 3 - 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.NumPets > 1 && c.NumPets < 4)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Select(c => c.NumPets <= 1 || c.NumPets >= 3)
            )));
        }

        [TestMethod]
        public void Select_ConcreteType()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Select(c => new DummyClass {
                    Id = c.NumPets,
                    MyName = c.FirstName,
                    MyNumPets = c.NumPets
                })
            )));
        }

    }
}
