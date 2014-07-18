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
using MongoDB.Bson.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace MongoLinqPlusPlus
{
    /// <summary>
    /// Class to enable Json.Net to deserialize groupings.
    /// TODO: Revisit this code.
    /// </summary>
    public class GroupingConverter : CustomCreationConverter<object>
    {
        private Type _mongoDocType;
        private string PIPELINE_DOCUMENT_RESULT_NAME = MongoPipeline<int>.PIPELINE_DOCUMENT_RESULT_NAME;

        public GroupingConverter(Type mongoDocType)
        {
            _mongoDocType = mongoDocType;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType.FullName.StartsWith("System.Linq.IGrouping`2");
        }

        public override object Create(Type objectType)
        {
            var retval = objectType.GetConstructor(new Type[0]).Invoke(new object[0]);
            return retval;
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            // Create a new return value with the right type
            var targetType = typeof(MyGrouping<,>).MakeGenericType(objectType.GetGenericArguments());
            var target = Activator.CreateInstance(targetType);

            // Load JObject from stream
            JObject jObject = JObject.Load(reader);

            // Populate the key (which maps to the mongo _id field)
            var keyProperty = targetType.GetProperty("Key");
            var keyValue = jObject["_id"].ToObject(objectType.GenericTypeArguments[0]);
            keyProperty.SetValue(target, keyValue);

            // Populate the values
            foreach (var jObj in jObject["Values"])
            {
                object asObject;

                // See if it's exactly our Mongo doc type OR if it's an interface implemented by our mongo doc
                if (_mongoDocType == objectType.GenericTypeArguments[1]
                    || _mongoDocType.GetInterfaces().Contains(objectType.GenericTypeArguments[1]))
                {
                    asObject = BsonSerializer.Deserialize(jObj.ToString(), _mongoDocType);
                }
                else
                {
                    // See if we're deserializing a value type like: { _result_ = "foo" }
                    if (jObj.Count() == 1 && jObj[PIPELINE_DOCUMENT_RESULT_NAME] != null)
                    {
                        asObject = jObj[PIPELINE_DOCUMENT_RESULT_NAME].ToObject(objectType.GenericTypeArguments[1]);
                    }
                    else
                    {
                        asObject = jObj.ToObject(objectType.GenericTypeArguments[1]);
                    }
                }
                var list = targetType.GetProperty("Values").GetValue(target);
                list.GetType().GetMethod("Add").Invoke(list, new[] { asObject });
            }

            return target;
        }
    }


    public class MyGrouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        public MyGrouping()
        {
            Values = new List<TElement>();
        }

        public List<TElement> Values { get; set; }
        public TKey Key { get; set; }

        public IEnumerator<TElement> GetEnumerator()
        {
            return Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}

