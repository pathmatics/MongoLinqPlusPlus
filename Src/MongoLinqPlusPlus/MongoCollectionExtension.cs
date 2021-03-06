﻿// The MIT License (MIT)
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
using System.Linq.Expressions;
using MongoDB.Driver;

namespace MongoLinqPlusPlus
{
    /// <summary>
    /// Extend a MongoCollection with our own Queryable that utilizes the Aggregation Framework
    /// </summary>
    public static class MongoExtensions
    {
        /// <summary>
        /// Gets our custom Aggregation Framework based queryable from a MongoCollection.
        /// </summary>
        /// <typeparam name="TMongoDocument">The document type stored in the collection</typeparam>
        /// <param name="collection">A Mongo collection to query</param>
        /// <returns>An IQueryable for running Linq queries against</returns>
        public static IQueryable<TMongoDocument> QueryablePlusPlus<TMongoDocument>(this IMongoCollection<TMongoDocument> collection)
        {
            return collection.QueryablePlusPlus(false, null);
        }

        /// <summary>
        /// Gets our custom Aggregation Framework based queryable from a MongoCollection
        /// </summary>
        /// <typeparam name="TMongoDocument">The document type stored in the collection</typeparam>
        /// <param name="collection">A Mongo collection to query</param>
        /// <param name="allowMongoDiskUse">
        /// If true, MongoDB can use the disk for the query if possible.  If false it won't but there's
        /// a risk that the query is too big to be handled in memory.
        /// </param>
        /// <returns>An IQueryable for running Linq queries against</returns>
        public static IQueryable<TMongoDocument> QueryablePlusPlus<TMongoDocument>(this IMongoCollection<TMongoDocument> collection, bool allowMongoDiskUse)
        {
            return collection.QueryablePlusPlus(allowMongoDiskUse, null);
        }

        /// <summary>
        /// Gets our custom Aggregation Framework based queryable from a MongoCollection
        /// </summary>
        /// <typeparam name="TMongoDocument">The document type stored in the collection</typeparam>
        /// <param name="collection">A Mongo collection to query</param>
        /// <param name="loggingDelegate">Callback function for debug logging</param>
        /// <returns>An IQueryable for running Linq queries against</returns>
        public static IQueryable<TMongoDocument> QueryablePlusPlus<TMongoDocument>(this IMongoCollection<TMongoDocument> collection, Action<string> loggingDelegate)
        {
            return collection.QueryablePlusPlus(false, loggingDelegate);
        }

        /// <summary>
        /// Gets our custom Aggregation Framework based queryable from a MongoCollection
        /// </summary>
        /// <typeparam name="TMongoDocument">The document type stored in the collection</typeparam>
        /// <param name="collection">A Mongo collection to query</param>
        /// <param name="allowMongoDiskUse">
        /// If true, MongoDB can use the disk for the query if possible.  If false it won't but there's
        /// a risk that the query is too big to be handled in memory.
        /// </param>
        /// <param name="loggingDelegate">Callback function for debug logging</param>
        /// <returns>An IQueryable for running Linq queries against</returns>
        public static IQueryable<TMongoDocument> QueryablePlusPlus<TMongoDocument>(this IMongoCollection<TMongoDocument> collection, bool allowMongoDiskUse, Action<string> loggingDelegate)
        {
            var queryable = new MongoAggregationQueryable<TMongoDocument>(collection.CollectionNamespace.CollectionName);

            queryable.Expression = Expression.Constant(queryable);
            queryable.Provider = new MongoAggregationQueryProvider<TMongoDocument>(collection) {
                Queryable = queryable,
                LoggingDelegate = loggingDelegate,
                AllowMongoDiskUse = allowMongoDiskUse
            };

            return queryable;
        }
    }
}
