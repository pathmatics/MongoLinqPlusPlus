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
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoLinqPlusPlus
{
    /// <summary>Extend the Type class</summary>
    public static class Extensions
    {
        /// <summary>Returns true if this type is Anonymous, otherwise false.</summary>
        public static bool IsAnonymousType(this Type type)
        {
            bool hasCompilerGeneratedAttribute = type.GetCustomAttributes(typeof(CompilerGeneratedAttribute), false).Any();
            bool nameContainsAnonymousType = type.FullName.Contains("AnonymousType");
            bool isAnonymousType = hasCompilerGeneratedAttribute && nameContainsAnonymousType;

            return isAnonymousType;
        }

        /// <summary>
        /// Returns true iff this type is a non-nullable value type.
        /// (Not that this behavior differs from the property Type.IsValueType)
        /// </summary>
        public static bool IsNonNullableValueType(this Type type)
        {
            return type.IsValueType && Nullable.GetUnderlyingType(type) == null;
        }

        /// <summary>
        /// Flips the binary operator.
        /// Examples: LessThan becomes GreaterThan.  Equal remains Equal.
        /// Throws ArgumentOutOfRangeException if not implemented or not supported
        /// </summary>
        public static ExpressionType Flip(this ExpressionType e)
        {
            if (e == ExpressionType.GreaterThan)
                return  ExpressionType.LessThan;
            
            if (e == ExpressionType.GreaterThanOrEqual)
                return  ExpressionType.LessThanOrEqual;
            
            if (e == ExpressionType.LessThan)
                return ExpressionType.GreaterThan;
            
            if (e == ExpressionType.LessThanOrEqual)
                return ExpressionType.GreaterThanOrEqual;
            
            if (e == ExpressionType.Equal)
                return ExpressionType.Equal;

            throw new NotSupportedException($"Can't (or haven't implemented) flipping ExpressionType {e}");
        }
        /// <summary>
        /// In replace of deprecated constructor with manually assigning pid, machine, increment
        /// </summary>
        /// <param name="timestamp">creation time</param>
        /// <param name="manuallyAssignRandomBytesHexString">random bytes</param>
        /// <returns></returns>
        public static ObjectId GenerateNewIdWithAssigningRandomBytes(DateTime timestamp, string manuallyAssignRandomBytesHexString)
        {
            var baseId = ObjectId.GenerateNewId(timestamp).ToByteArray();
            var randomBytes = StringToByteArray(manuallyAssignRandomBytesHexString);
            if (randomBytes.Length != 8) throw new InvalidOperationException("Invalid hex string to assign!");
            Array.Copy(randomBytes, 0, baseId, baseId.Length - 8, 8);
            return new ObjectId(baseId);
        }

        public static byte[] StringToByteArray(string hex)
        {
            return Enumerable.Range(0, hex.Length)
                .Where(x => x % 2 == 0)
                .Select(x => Convert.ToByte(hex.Substring(x, 2), 16))
                .ToArray();
        }
    }
}
