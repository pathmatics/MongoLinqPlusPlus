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
using System.Linq;
using System.Linq.Expressions;
using MongoDB.Driver;

namespace MongoLinqPlusPlus
{
    /// <summary>
    /// Custom IQueryProvider that translates Expressions to MongoDB Aggregation framework queries
    /// </summary>
    /// <typeparam name="TDocument">The Mongo Document type to query against</typeparam>
    public class MongoAggregationQueryProvider<TDocument> : IQueryProvider
    {
        /// <summary>The Mongo collection to query against</summary>
        private MongoCollection<TDocument> _collection;

        public object Queryable { get; set; }
        public Action<string> LoggingDelegate { get; set; }

        /// <summary>
        /// Constructs a new provider from a Mongo collection.  No need to call this directly,
        /// instead, use the QueryablePlusPlus extension a Mongo Collection
        /// </summary>
        /// <param name="collection"></param>
        public MongoAggregationQueryProvider(MongoCollection<TDocument> collection)
        {
            _collection = collection;
        }

        /// <summary>No need to call this directly, required of IQueryProvider</summary>
        public IQueryable<TResult> CreateQuery<TResult>(Expression expression)
        {
            if (!typeof(IQueryable<TResult>).IsAssignableFrom(expression.Type))
                throw new ArgumentOutOfRangeException("expression");

            var queryable = new MongoAggregationQueryable<TResult> {
                Provider = this,
                Expression = expression
            };

            return queryable;
        }

        /// <summary>
        /// Executes the actual query.  Called automatically when the query is evaluated
        /// </summary>
        public TResult Execute<TResult>(Expression expression)
        {
            LoggingDelegate("\r\n----------------- EXPRESSION --------------------\r\n\r\n");
            var localExpression = expression;
            LoggingDelegate(expression + "\r\n");

            // Reduce any parts of the expression that can be evaluated locally
            var simplifiedExpression = ExpressionSimplifier.Simplify(this.Queryable, localExpression);
            if (simplifiedExpression != localExpression)
            {
                LoggingDelegate("\r\n----------------- SIMPLIFIED EXPRESSION --------------------\r\n\r\n");
                localExpression = simplifiedExpression;
                LoggingDelegate(localExpression + "\r\n");
            }

            var pipeline = new MongoPipeline<TDocument>(_collection, LoggingDelegate);
            return pipeline.Execute<TResult>(localExpression);
        }

        /// <summary>I haven't seen this called yet...</summary>
        public IQueryable CreateQuery(Expression expression)
        {
            throw new NotImplementedException();
        }

        /// <summary>I haven't seen this called yet...</summary>
        public object Execute(Expression expression)
        {
            throw new NotImplementedException();
        }
    }
}