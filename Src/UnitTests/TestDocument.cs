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

using System;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoLinqPlusPlus.Tests
{
    public enum States
    {
        CA, NY, NJ, WA, IL
    }

    public class Address
    {
        public string Street;
        public int Apartment;
        public string City;
        public States State;
        public int Zip;
    }

    public class TestDocument
    {
        [BsonId] public string SSN;
        [BsonElement("first")] public string FirstName;
        [BsonElement("last")] public string LastName;
        public bool IsMale;
        public DateTime Birthday;
        public Address CurrentAddress;
        public Address[] PreviousAddresses;
        public int NumPets;
        public long StudentId;
        public double GPA;

        public TestDocument CloneWithNewSSN(int seed)
        {
            return new TestDocument {
                SSN = seed.ToString("000000000").Insert(5, "-").Insert(3, "-").Substring(0, 11),
                LastName = this.LastName,
                FirstName = this.FirstName,
                IsMale = this.IsMale,
                Birthday = this.Birthday,
                CurrentAddress = this.CurrentAddress,
                PreviousAddresses = this.PreviousAddresses,
                NumPets = this.NumPets,
                StudentId = this.StudentId,
                GPA = this.GPA
            };
        }
    }
}
