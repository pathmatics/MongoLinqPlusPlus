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
using UnitTests;

namespace MongoLinqPlusPlus.Tests
{
    [TestClass]
    public class JoinTests
    {
        private IQueryable<TestDocument> _mongoQuery = TestRepository.GetDefaultDataQueryablePlusPlus(s => System.Diagnostics.Debug.Write(s));
        private IQueryable<Student> _mongoStudentQuery = StudentRepository.GetDefaultDataQueryablePlusPlus(s => System.Diagnostics.Debug.Write(s));
        private IQueryable<TestDocument> _memryQuery = TestRepository.TestDocuments.AsQueryable();
        private IQueryable<Student> _memryStudentQuery = StudentRepository.TestDocuments.AsQueryable();

        [TestMethod]
        public void JoinSingleKey()
        {
            var inputs = new[] {(people: _mongoQuery, students:_mongoStudentQuery), (people: _memryQuery, students: _memryStudentQuery)};

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                       p => p.FirstName,
                       s => s.FirstName,
                       (p, s) => p.SSN
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => p.FirstName,
                     s => s.FirstName,
                     (p, s) => p.FirstName
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => p.FirstName,
                     s => s.FirstName,
                     (p, s) => p.CurrentAddress
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => p.LastName,
                     s => s.FirstName,
                     (p, s) => p.SSN
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => p.StudentId,
                     s => s.StudentNum,
                     (p, s) => p.SSN
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => p.StudentId,
                     s => s.StudentNum,
                     (p, s) => new {
                         p.FirstName,
                         JoinedFirstName = s.FirstName,
                         p.StudentId,
                         s.StudentNum,
                         s.University,
                         p.LastName
                     })
            )));
        }

        [TestMethod]
        public void JoinCompoundKey()
        {
            var inputs = new[] {(people: _mongoQuery, students:_mongoStudentQuery), (people: _memryQuery, students: _memryStudentQuery)};

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => new { p.FirstName },
                     s => new { s.FirstName },
                     (p, s) => p.SSN
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => new { p.FirstName, p.LastName },
                     s => new { s.FirstName, LastName = s.FirstName },
                     (p, s) => p.FirstName
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => new { p.FirstName, p.LastName, p.StudentId },
                     s => new { s.FirstName, LastName = s.FirstName, StudentId = s.StudentNum },
                     (p, s) => p.CurrentAddress
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => new { p.FirstName, p.LastName, Num = p.StudentId },
                     s => new { s.FirstName, LastName = s.FirstName, Num = s.StudentNum },
                     (p, s) => p.SSN
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => p.StudentId,
                     s => s.StudentNum,
                     (p, s) => p.SSN
                 )
            )));

            Assert.IsTrue(TestHelpers.AreEqual(inputs.Select(c =>
                c.people
                 .Join(c.students,
                     p => p.StudentId,
                     s => s.StudentNum,
                     (p, s) => new {
                         p.FirstName,
                         JoinedFirstName = s.FirstName,
                         p.StudentId,
                         s.StudentNum,
                         s.University,
                         p.LastName
                     })
            )));

        }
    }
}
