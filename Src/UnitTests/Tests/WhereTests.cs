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
// ReSharper disable StringCompareIsCultureSpecific.1

namespace MongoLinqPlusPlus.Tests
{
    [TestClass]
    public class WhereTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestRepository.GetDefaultDataQueryablePlusPlus(s => System.Diagnostics.Debug.Write(s));
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
                queryable.Where(c => c.CurrentAddress.State == States.CA || c.CurrentAddress.State == States.WA)
            )));
        }

        [TestMethod]
        public void Where_Operators()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.IsMale && c.NumPets > 2)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => !c.IsMale || c.NumPets >=2)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => !(c.IsMale || c.NumPets < 2))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName != "Harry"  && !(c.NumPets <= 2))
            )));
        }

        [TestMethod]
        public void Where_Array_Any_Contains()
        {
            var ids = new[] {1, 2};
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null && c.OldIds.Any(d => ids.Contains(d)))
            )));

            var zips = new[] {90405, 90401};
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Any(d => zips.Contains(d.Zip)))
            )));
        }

        [TestMethod]
        public void Where_Array_Any()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null && c.OldIds.Any(d => d == 4))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Any(d => d.Zip == 90405))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Any(d => d.Zip == 90405 || d.State == States.CA))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null && c.OldIds.Any(d => d != 4 && d > 1))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null && c.OldIds.Any())
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null)
                         .Select(c => c.OldIds)
                         .Where(c => c.Any())
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null)
                         .Select(c => c.OldIds)
                         .Where(c => c.Any(d => d >= 2))
            )));
        }
        
        [TestMethod]
        public void Where_Array_All()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null && c.OldIds.All(d => d == 4))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.OldIds != null && c.OldIds.All(d => d < 4))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.All(d => d.Zip != 90405))
            )));
        }


        [TestMethod]
        public void Where_Array_Length()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Length == 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Length > 1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Length < 2)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Length >= 2)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Length <= 2)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.PreviousAddresses.Length != 2)
            )));
        }

        [TestMethod]
        public void Where_Complex()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => (c.FirstName != "John" || c.FirstName == "Bob" || c.FirstName == "June")
                                && (c.IsMale || !c.IsMale && c.NumPets >= 1)
                                && c.CurrentAddress.State != States.WA)
            )));
        }

        [TestMethod]
        public void Where_Chained()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName == "Tom").Where(c => c.CurrentAddress.State == States.WA).Where(c => true).Where(c => c.NumPets != 3)
            )));
        }

        [TestMethod]
        public void Where_Static()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.Birthday < DateTime.Today)
            )));
        }

        [TestMethod]
        public void Where_ConstantArrayContains()
        {
            int[] numPetsArray = {2, 3, 4};
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => numPetsArray.Contains(c.NumPets))
            )));

            States[] statesArray = {States.WA, States.CA};
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => statesArray.Contains(c.CurrentAddress.State))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => new[] { 2, 3, 4 }.Contains(c.NumPets))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => new[] { States.WA, States.CA }.Contains(c.CurrentAddress.State))
            )));
        }

        [TestMethod]
        public void Where_String_IsNullOrEmpty()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => string.IsNullOrEmpty(c.FirstName))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => string.IsNullOrEmpty(c.LastName))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => string.IsNullOrEmpty(c.FirstName) && string.IsNullOrEmpty(c.LastName))
            )));
        }

        [TestMethod]
        public void Where_String_Compare()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => string.Compare(c.FirstName, c.LastName) == 0)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => string.Compare(c.FirstName, c.LastName) == -1)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => string.Compare(c.FirstName, c.LastName) > 0)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => string.Compare(c.FirstName, "Tom") >= 0)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => string.Compare("Tom", c.LastName) <= 0)
            )));
        }

        [TestMethod]
        public void Where_String_Contains()
        {
            // We have a null first name in our test data.  Mongo will handle this ok but Linq-to-objects will blow up
            // So check for null to make this test pass.
            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.FirstName != null && c.FirstName.Contains("Tom"))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] { _mongoQuery, _memryQuery }.Select(queryable =>
                queryable.Where(c => c.LastName.Contains("o"))
            )));
        }

        [TestMethod]
        public void Where_Advanced_Expr_Expressions()
        {
            // In MongoDB 3.6 we can use {$match, {$expr, ... }} to actually use real expressions in our where queries
            // Test that here

            try
            {
                Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                    queryable.Where(c => c.FirstName == c.LastName)
                )));

                Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                    queryable.Where(c => 4 == c.NumPets)
                )));

                Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                    queryable.Where(c => c.FirstName != c.LastName)
                )));

                Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                    queryable.Where(c => c.FirstName != null && c.FirstName.Length > c.NumPets + 2)
                )));
            }
            catch (InvalidQueryException)
            {
                // Not yet supported :(
            }
        }

        [TestMethod]
        public void Where_Exception()
        {
            // Confirm that we rethrow the correct exception type from inside the pipeline
            try
            {
                int[] array = {1, 2};
                Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                    queryable.Where(c => c.NumPets == array[3])
                    )));
                Assert.Fail("This is supposed to fail...");
            }
            catch (IndexOutOfRangeException)
            {
                // Success!
            }
        }

        [TestMethod]
        public void Where_Nullable()
        {
            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.NullableInt < 10)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.NullableInt > 10)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.NullableInt == 5)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.NullableInt != null && c.NullableInt.Value == 5)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.NullableInt != null && new[] { 5,10}.Contains(c.NullableInt.Value))
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.NullableInt == null)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.NullableInt != null)
            )));

            Assert.IsTrue(TestHelpers.AreEqual(new[] {_mongoQuery, _memryQuery}.Select(queryable =>
                queryable.Where(c => c.NullableInt > c.NumPets)
            )));
        }
    }
}
