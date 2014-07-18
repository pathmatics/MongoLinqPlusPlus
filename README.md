# MongoLinqPlusPlus#

A .NET LINQ provider for MongoDB that utilizes the MongoDB Aggregation Framework 

### Why is this useful? ###

The LINQ provider that ships with the MongoDB C# driver is limited in it's functionality.
The MongoDB Aggregation Framework adds all kinds of goodness that was just waiting to be
wrapped by a new LINQ provider.  The main benefits of MongoLinqPlusPlus are groupings
(.GroupBy) and projections (.Select).

MongoLinqPlusPlus is still under development.  Use at your own risk.

### What operations are currently supported? ###
* .Where
* .Where(localEnumerable.Contains())
* .Select
* .GroupBy
* .GroupBy().Select(group aggregation)
* .OrderBy
* .OrderByDescending
* .ThenBy
* .ThenByDescending
* .Sum
* .Min
* .Max
* .Average
* .Count
* .Any

More features are still being added (string operations, First(), Single(), etc).

### Dependencies ###

* .Net 4.5
* MongoDB 2.6 server
* MongoDB 1.9 .NET client
* Json.NET

### Building ###
The source should build cleanly in VisualStudio 2013 without jumping through any hoops.

### Experimenting ###
There's a set of unit tests and a very basic test program you can play with.  They
require a MongoDB server on localhost:27017 without auth.  You can specify your
own connection string in TestRespository.cs.

### Using in your own code ###
Add a reference and using directive to MongoLinqPlusPlus.  The provider is
accessed via the QueryablePlusPlus extension method on MongoCollection<T>.


```
#!c#

#using MongoLinqPlusPlus

MongoCollection<T> collection;
var results = collection.QueryablePlusPlus().Where(c => c.Age > 31);

```

### Support ###
Feel free to open bugs you find.  Keep in mind that functionality is limited by what is supported by the [Aggregation Framework](http://docs.mongodb.org/manual/meta/aggregation-quick-reference/).

Cheers.

Tom Lorimor  
Adomic, Inc

### License ###
The MIT License (MIT)

Copyright (c) 2014 Adomic, Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.