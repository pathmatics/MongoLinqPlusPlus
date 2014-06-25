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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace MongoLinqPlusPlus.Tests
{
    /// <summary>Helpers for our unit tests</summary>
    public static class TestHelpers
    {
        /// <summary>Initialize our Mongo database with fresh data and get an IQueryable to it</summary>
        public static IQueryable<TestDocument> InitMongo()
        {
            var repo = new TestRepository();
            repo.Collection.Drop();
            repo.LoadTestData();
            return new TestRepository().Collection.AsAggregationQueryable();
        }

        /// <summary>
        /// My own implementation of GetHashCode since the built in function wasn't giving me different
        /// results for objects which looked the same to my eyeballs.  (Perhaps including private members?)
        /// </summary>
        private static int MyGetHashCode(object obj)
        {
            if (obj == null)
                return 0;

            unchecked
            {
                var type = obj.GetType();

                // Handle any well known type with the default implementation
                if (obj is int || obj is long || obj is bool || obj is char || obj is byte || obj is DateTime || obj is double || obj is float || obj is string || obj is Enum)
                    return obj.GetHashCode();

                int hashCode = 0;

                // Handle an enumerable type
                if (obj is IEnumerable<object>)
                {
                    foreach (var subItem in (IEnumerable<object>) obj)
                        hashCode += MyGetHashCode(subItem);

                    // For IGrouping, include the Key property as well
                    var keyProperty = type.GetProperty("Key");
                    if (keyProperty != null && keyProperty.CanRead)
                    {
                        hashCode += MyGetHashCode(keyProperty.GetMethod.Invoke(obj, null));
                    }

                    return hashCode;
                }

                // Sum public properties
                foreach (var subItem in type.GetProperties()
                                            .Where(c => c.CanRead)
                                            .Select(c => c.GetMethod.Invoke(obj, null)))
                {
                    hashCode += MyGetHashCode(subItem);
                }

                // Sum public fields
                foreach (var subItem in type.GetFields()
                                           .Where(c => c.IsPublic)
                                           .Select(c => c.GetValue(obj)))
                {
                    hashCode += MyGetHashCode(subItem);
                }

                return hashCode;
            }
        }

        /// <summary>
        /// Accepts an IEnumerable of two objects (which might be unevaluated queries) and confirms they contains the same type and underlying data.
        /// </summary>
        /// <param name="objects">IEnumerable of exactly two objects</param>
        /// <returns></returns>
        public static bool AreEqual(IEnumerable<object> objects)
        {
            var objectArray = objects.ToArray();
            if (objectArray.Length != 2)
                throw new ArgumentException("Can only compare 2 objects.");

            var areEqual = AreEqual(objectArray[0], objectArray[1]);
            if (areEqual)
                return true;

            // FAIL.  Output the results sets for visual inspection.

            // Boo :(  Run our query again to we can output the JSON.
            object mongoResultObject = objectArray[0] is IEnumerable<object> ? ((IEnumerable<object>) objectArray[0]).ToArray() : objectArray[0];
            object memryResultObject = objectArray[1] is IEnumerable<object> ? ((IEnumerable<object>) objectArray[1]).ToArray() : objectArray[1];

            var mongoJson = JsonConvert.SerializeObject(mongoResultObject, Formatting.Indented);
            var memryJson = JsonConvert.SerializeObject(memryResultObject, Formatting.Indented);

            // Display results in 2 columns!!!
            var mongoLines = mongoJson.Split(new[] {'\r', '\n'}, StringSplitOptions.RemoveEmptyEntries).Select(c => c.PadRight(50)).ToList();
            var memryLines = memryJson.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList();

            // Make sure each has the same number of lines
            mongoLines.AddRange(Enumerable.Range(0, Math.Max(0, memryLines.Count() - mongoLines.Count())).Select(c => "".PadRight(50)));
            memryLines.AddRange(Enumerable.Range(0, Math.Max(0, mongoLines.Count() - memryLines.Count())).Select(c => ""));

            Console.WriteLine("\r\n{0}{1}", "---------- Mongo Result Json ------------".PadRight(50), "---------- Memry Result Json ------------");
            var text = string.Join("\r\n", mongoLines.Select((c, i) => c + memryLines[i]));
            Console.WriteLine(text);

            return false;
        }

        /// <summary>Checks if two objects are of the same type and contain the same data.</summary>
        /// <param name="mongoObj">Object (which might be unevaluated IEnumerable) contains results of Mongo query</param>
        /// <param name="memryObj">Object (which might be unevaluated IEnumerable) contains results of in memory query</param>
        /// <returns>True if they contain the same type and data, otherwise false.</returns>
        private static bool AreEqual(object mongoObj, object memryObj)
        {
            // Handle an enumerable type
            if (mongoObj is IEnumerable && memryObj is IEnumerable)
            {
                bool isOrdered = mongoObj is IOrderedEnumerable<object>;

                // TODO: What about IGrouping?

                // Enumerate it!
                var mongoArray = ((IEnumerable) mongoObj).Cast<object>().ToArray();
                var memryArray = ((IEnumerable) memryObj).Cast<object>().ToArray();

                if (mongoArray.Length != memryArray.Length)
                    return false;

                // If we weren't already ordered, order it!
                if (!isOrdered)
                {
                    mongoArray = mongoArray.Select(c => new {
                                               Obj = c,
                                               HashCode = MyGetHashCode(c)
                                           })
                                           .OrderBy(c => c.HashCode)
                                           .Select(c => c.Obj)
                                           .ToArray();

                    memryArray = memryArray.Select(c => new {
                                               Obj = c,
                                               HashCode = MyGetHashCode(c)
                                           })
                                           .OrderBy(c => c.HashCode)
                                           .Select(c => c.Obj)
                                           .ToArray();
                }

                // Make sure that the elements at each position match
                for (int i = 0; i < mongoArray.Length; i++)
                    if (!AreEqual(mongoArray[i], memryArray[i]))
                        return false;

                return true;
            }

            if (mongoObj.GetType() != memryObj.GetType())
                throw new InvalidCastException("Mongo and memory object types don't match.");

            var type = mongoObj.GetType();

            // Handle any simple type
            if (mongoObj is int || mongoObj is long || mongoObj is bool || mongoObj is char || mongoObj is byte || mongoObj is DateTime || mongoObj is double || mongoObj is float || mongoObj is string || mongoObj is Enum)
                return mongoObj.Equals(memryObj);

            // Check each property
            foreach (var property in type.GetProperties().Where(c => c.CanRead))
            {
                var mongoPropertyValue = property.GetMethod.Invoke(mongoObj, new object[0]);
                var memryPropertyValue = property.GetMethod.Invoke(memryObj, new object[0]);

                if (!AreEqual(mongoPropertyValue, memryPropertyValue))
                    return false;
            }

            // Check each field
            foreach (var field in type.GetFields().Where(c => c.IsPublic))
            {
                var mongoFieldValue = field.GetValue(mongoObj);
                var memryFieldValue = field.GetValue(mongoObj);

                if (!AreEqual(mongoFieldValue, memryFieldValue))
                    return false;
            }

            return true;
        }
    }
}
