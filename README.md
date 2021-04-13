# MongoLinqPlusPlus

![MasterPublish](https://github.com/pathmatics/MongoLinqPlusPlus/workflows/MasterPublish/badge.svg)

A .NET LINQ provider for MongoDB that utilizes the MongoDB Aggregation Framework 

### Why is this useful? ###

This was written back when the the LINQ provider that ships with the MongoDB C# driver
was extremely limited in support.  Since then they have improved their LINQ support;
however, I've still found their LINQ support limited when compared to everything that
can be done via the Aggregation Framework.  MongoLinqPlusPlus was developed to support
our query pattern here at Pathmatics.  The main benefits of MongoLinqPlusPlus are groupings
(.GroupBy), projections (.Select), and aggregations (.Sum).  You can even mix them together:

    .Where(c => c.Age > 30)
    .GroupBy(c => c.Name)
    .Select(c => new {
        Name = c.Key,
        TotalPets = c.Sum(d => d.NumPets)
    })

MongoLinqPlusPlus is still under active development.  Use at your own risk.

### What operations are currently supported? ###
* .Where (including full expression support on 3.6 via $expr) 
* .Select
* .SelectMany
* .GroupBy
* .GroupBy().Select(aggregation methods on the group)
* .OrderBy
* .OrderByDescending
* .ThenBy
* .ThenByDescending
* .Take
* .Skip
* .Sum
* .Min
* .Max
* .Average
* .Count
* .Any
* .First
* .FirstOrDefault
* .Single
* .SingleOrDefault

More features are still being added as we need them.  All supported functionality is in the unit tests
and that has become the source of truth for what is truly supported.  

### Dependencies ###

* .Net 4.7
* MongoDB Mongo 3.6 server (with good support for 3.4)
* MongoDB 2.0 Legacy .NET client (does not support the new API yet)
* Json.NET

### Building ###
The source should build cleanly in VisualStudio 2017 without jumping through any hoops.

### Experimenting ###
There's a set of unit tests and a very basic test program you can play with.  They
require a MongoDB server on localhost:27017 without auth.  You can specify your
own connection string in TestRespository.cs.

### Using in your own code ###
Add a reference and using directive to MongoLinqPlusPlus.  The provider is
accessed via the QueryablePlusPlus extension method on MongoCollection<T>.
Check out the overloads on QueryablePlusPlus() for some advanced functionality.


```

#using MongoLinqPlusPlus

MongoCollection<T> collection;
var results = collection.QueryablePlusPlus().Where(c => c.Age > 31).ToList();

// Get debug output
int sum = collection.QueryablePlusPlus(Console.WriteLine).Sum(c => c.NumPets);


```

### Support ###
Feel free to open bugs you find.  Keep in mind that functionality is limited by what is supported by the [Aggregation Framework](http://docs.mongodb.org/manual/meta/aggregation-quick-reference/).

Cheers.

Tom Lorimor  
Pathmatics, Inc

### License ###
The MIT License (MIT)

Copyright (c) 2021 Pathmatics, Inc

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
