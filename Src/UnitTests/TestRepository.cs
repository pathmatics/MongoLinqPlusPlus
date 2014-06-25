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
using MongoDB.Driver;

namespace MongoLinqPlusPlus.Tests
{
    /// <summary>
    /// Test repository that connects to a mongo server running on localhost
    /// </summary>
    public partial class TestRepository
    {
        public MongoDatabase Database
        {
            get
            {
                MongoDefaults.MaxConnectionIdleTime = TimeSpan.FromMinutes(1);
                var client = new MongoClient("mongodb://localhost");
                return client.GetServer().GetDatabase("mongoLinqPlusPlus");
            }
        }

        public MongoCollection<TestDocument> Collection { get { return Database.GetCollection<TestDocument>("test"); } }

        public void LoadTestData()
        {
            foreach (var doc in TestDocuments)
                this.Collection.Insert(doc);
        }
    }
}