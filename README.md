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

### Dependencies ###

* .Net 4.5
* MongoDB 2.6 server
* MongoDB 1.9 .NET client
* Json.NET

### Building ###
The source should build cleanly in VisualStudio 2013 without jumping through any hoops.

### Experimenting ###
There's a set of unit tests and a very basic test program you can play with.  They
require a MongoDB server on localhost:27017 without auth.  Feel free to specify your
own connection string in TestRespository.cs though.

### Using in your own code ###
Simple add a reference and using directive to MongoLinqPlusPlus.  The provider is
accessed via the QueryablePlusPlus extension method on MongoCollection<T>.


```
#!c#

#using MongoLinqPlusPlus

MongoCollection<T> collection = ...
var results = collection.QueryablePlusPlus().Where(c => c.Age > 31)

```
