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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace MongoLinqPlusPlus
{
    /// <summary>
    /// Dummy implementation of IQueryable for use by our own Linq Provider
    /// </summary>
    internal class MongoAggregationQueryable<TData> : IOrderedQueryable<TData>
    {
        public IEnumerator<TData> GetEnumerator()
        {
            return Provider.Execute<IEnumerable<TData>>(Expression).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Provider.Execute<IEnumerable>(Expression).GetEnumerator();
        }

        public Expression Expression { get; internal set; }
        public Type ElementType => typeof(TData);
        public IQueryProvider Provider { get; internal set; }
    }
}
