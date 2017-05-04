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

namespace MongoLinqPlusPlus.Tests
{
    public partial class TestRepository
    {
        private static Address[] Addresses = {
            new Address { Street = "2001 4th St", City = "Santa Monica", Zip = 90405, State = States.CA },
            new Address { Street = "41 Santa Monica Blvd", City = "Santa Monica", Zip = 90401, State = States.CA },
            new Address { Street = "230 Pacific St", Apartment = 203, City = "Santa Monica", Zip = 90405, State = States.CA },
            new Address { Street = "230 Pacific St", Apartment = 301, City = "Santa Monica", Zip = 90405, State = States.CA },
            new Address { Street = "12 Smokerise Dr", City = "Warren", Zip = 07059, State = States.NJ },
            new Address { Street = "21541 E. Greenlake Way", City = "Seattle", Zip = 98106, State = States.WA },
            new Address { Street = "33333 E. Greenlake Way", Apartment = 15, City = "Seattle", Zip = 98106, State = States.WA },
        };

        public static TestDocument[] TestDocuments = {
            new TestDocument {
                SSN = "000-00-0000",
                FirstName = "Tom",
                LastName = "Jones",
                IsMale = true,
                Birthday = new DateTime(1988, 1, 1).ToUniversalTime(),
                CurrentAddress = Addresses[0],
                NumPets = 1,
                PreviousAddresses = new[] { Addresses[1], Addresses[2] },
                StudentId = 5012050215,
                GPA = 4.0,
                Guid = Guid.Parse("00000001-0001-0001-0001-000000000001")
            },
            new TestDocument {
                SSN = "000-00-0001",
                FirstName = "Tom",
                LastName = "Bosley",
                IsMale = true,
                Birthday = new DateTime(1988, 5, 12).AddMilliseconds(1295129).ToUniversalTime(),
                CurrentAddress = Addresses[1],
                NumPets = 2,
                PreviousAddresses = new[] { Addresses[1], Addresses[2] },
                StudentId = 12,
                GPA = 0.5,
                Guid = Guid.Parse("00000001-0001-0001-0001-000000000001")
            },
            new TestDocument {
                SSN = "000-00-0002",
                FirstName = "Tom",
                LastName = "Tom",
                IsMale = true,
                Birthday = new DateTime(1978, 1, 3).ToUniversalTime(),
                CurrentAddress = Addresses[6],
                NumPets = 3,
                PreviousAddresses = new[] { Addresses[3], Addresses[4] },
                StudentId = -1,
                GPA = 0,
                Guid = Guid.Parse("00000001-0001-0001-0001-000000000001")
            },
            new TestDocument {
                SSN = "000-00-0003",
                FirstName = "Bobby",
                LastName = "Jones",
                IsMale = true,
                Birthday = new DateTime(1980, 1, 24).ToUniversalTime(),
                CurrentAddress = Addresses[3],
                NumPets = 2,
                PreviousAddresses = new[] {Addresses[0]},
                StudentId = -1,
                GPA = 4,
                Guid = Guid.Parse("00000001-0001-0001-0001-000000000001")
            },
            new TestDocument {
                SSN = "000-00-0004",
                FirstName = "Frank",
                LastName = "Jones",
                IsMale = true,
                Birthday = new DateTime(1990, 5, 12).ToUniversalTime(),
                CurrentAddress = Addresses[4],
                NumPets = 1,
                PreviousAddresses = new Address[0],
                StudentId = 89958128951892,
                GPA = 3.1,
                Guid = Guid.Parse("00000002-0002-0002-0002-000000000002")
            },
            new TestDocument {
                SSN = "000-00-0005",
                FirstName = "Larry",
                LastName = "Wilcox",
                IsMale = true,
                Birthday = new DateTime(1995, 1, 1).ToUniversalTime(),
                CurrentAddress = Addresses[5],
                NumPets = 4,
                PreviousAddresses = new Address[0],
                StudentId = 0,
                GPA = 2.6,
                Guid = Guid.Parse("00000002-0002-0002-0002-000000000002")
            },
            new TestDocument {
                SSN = "000-00-0006",
                FirstName = "Erik",
                LastName = "Estrada",
                IsMale = true,
                Birthday = new DateTime(1995, 1, 1).ToUniversalTime(),
                CurrentAddress = Addresses[6],
                NumPets = 0,
                PreviousAddresses = new Address[0],
                StudentId = 55,
                GPA = 3.1,
                Guid = Guid.Parse("00000002-0002-0002-0002-000000000002")
            },
            new TestDocument {
                SSN = "000-00-0007",
                FirstName = "June",
                LastName = "Cleaver",
                IsMale = false,
                Birthday = new DateTime(1995, 5, 24).ToUniversalTime(),
                CurrentAddress = Addresses[0],
                NumPets = 0,
                PreviousAddresses = new[] { Addresses[1], Addresses[2], Addresses[3], Addresses[4], Addresses[5] },
                StudentId = long.MaxValue,
                GPA = 0.0012,
                Guid = Guid.Parse("00000002-0002-0002-0002-000000000002")
            },
            new TestDocument {
                SSN = "000-00-0008",
                FirstName = "Shelly",
                LastName = "Duvall",
                IsMale = false,
                Birthday = new DateTime(1985, 3, 1).ToUniversalTime(),
                CurrentAddress = Addresses[0],
                NumPets = 0,
                PreviousAddresses = new[] {Addresses[6]},
                StudentId = long.MinValue,
                GPA = 3.99999,
                Guid = Guid.Parse("00000002-0002-0002-0002-000000000002")
            },
            new TestDocument {
                SSN = "000-00-0009",
                FirstName = "Pat",
                LastName = "Boone",
                IsMale = true,
                Birthday = new DateTime(1981, 5, 12).ToUniversalTime(),
                CurrentAddress = Addresses[1],
                NumPets = 1,
                PreviousAddresses = new[] {Addresses[5]},
                StudentId = 20195092109509,
                GPA = 2.12,
                Guid = Guid.Parse("00000003-0003-0003-0003-000000000003")
            },
            new TestDocument {
                SSN = "000-00-0010",
                FirstName = "Pat",
                LastName = "Boone",
                IsMale = false,
                Birthday = new DateTime(1984, 6, 30).ToUniversalTime(),
                CurrentAddress = Addresses[2],
                NumPets = 0,
                PreviousAddresses = new[] {Addresses[4]},
                StudentId = 501205021522,
                GPA = 3.33333333,
                Guid = Guid.Parse("00000004-0004-0004-0004-000000000004")
            },
            new TestDocument {
                SSN = "000-00-0011",
                FirstName = null,
                LastName = "",
                IsMale = false,
                Birthday = new DateTime(1984, 6, 30).ToUniversalTime(),
                CurrentAddress = Addresses[2],
                NumPets = 0,
                PreviousAddresses = new[] {Addresses[4]},
                StudentId = 501205021522,
                NullableInt = 5,
                GPA = 3.33333333,
                Guid = Guid.Parse("00000005-0005-0005-0005-000000000005")
            }
        };
    }
}
