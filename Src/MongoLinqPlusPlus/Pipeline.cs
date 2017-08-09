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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using Newtonsoft.Json;

namespace MongoLinqPlusPlus
{
    [Flags]
    internal enum PipelineResultType
    {
        Enumerable              = 0x001,
        Aggregation             = 0x002,
        Grouped                 = 0x004,
        OneResultFromEnumerable = 0x008,
        OrDefault               = 0x010,
        First                   = 0x020,
        Single                  = 0x040,
        Any                     = 0x082,
    }

    internal class PipelineStage
    {
        public string PipelineOperator;
        public BsonValue Operation;
        public bool GroupNeedsCleanup;
    }

    internal class MongoPipeline<TDocType>
    {
        private Action<string> _loggingDelegate;
        internal const string PIPELINE_DOCUMENT_RESULT_NAME = "_result_";

        private JsonWriterSettings _jsonWriterSettings = new JsonWriterSettings {OutputMode = JsonOutputMode.Strict, Indent = true, NewLineChars = "\r\n"};

        private List<PipelineStage> _pipeline = new List<PipelineStage>();
        private PipelineResultType _lastPipelineOperation = PipelineResultType.Enumerable;
        private MongoCollection<TDocType> _collection;
        private readonly bool _allowMongoDiskUse;
        private int _nextUniqueVariableId = 0;

        /// <summary>
        /// Custom converters to use when utilizing Json.net for deserialization 
        /// </summary>
        private JsonConverter[] _customConverters = {
            new GroupingConverter(typeof(TDocType)),
            new BsonSerializerConverter<Guid>(),
            new BsonSerializerConverter<ObjectId>(),
            new BsonSerializerConverter<DateTime>()
        };

        private readonly Dictionary<ExpressionType, string> NodeToMongoQueryBuilderFuncDict = new Dictionary<ExpressionType, string> {
            {ExpressionType.Equal, "$eq"},
            {ExpressionType.NotEqual, "$ne"},
            {ExpressionType.GreaterThan, "$gt"},
            {ExpressionType.GreaterThanOrEqual, "$gte"},
            {ExpressionType.LessThan, "$lt"},
            {ExpressionType.LessThanOrEqual, "$lte"},
        };

        private readonly Dictionary<ExpressionType, Func<string, int, IMongoQuery>> NodeToMongoQueryBuilderArrayLengthFuncDict =
            new Dictionary<ExpressionType, Func<string, int, IMongoQuery>> {
            {ExpressionType.Equal, Query.Size},
            {ExpressionType.GreaterThan, Query.SizeGreaterThan},
            {ExpressionType.GreaterThanOrEqual, Query.SizeGreaterThanOrEqual},
            {ExpressionType.LessThan, Query.SizeLessThan},
            {ExpressionType.LessThanOrEqual, Query.SizeLessThanOrEqual},
        };

        private readonly Dictionary<ExpressionType, string> NodeToMongoBinaryOperatorDict = new Dictionary<ExpressionType, string> {
            {ExpressionType.Equal, "$eq"},
            {ExpressionType.NotEqual, "$ne"},
            {ExpressionType.AndAlso, "$and"},
            {ExpressionType.OrElse, "$or"},
            {ExpressionType.GreaterThan, "$gt"},
            {ExpressionType.GreaterThanOrEqual, "$gte"},
            {ExpressionType.LessThan, "$lt"},
            {ExpressionType.LessThanOrEqual, "$lte"},
            {ExpressionType.Add, "$add"},
            {ExpressionType.Subtract, "$subtract"},
            {ExpressionType.Multiply, "$multiply"},
            {ExpressionType.Divide, "$divide"},
            {ExpressionType.Modulo, "$mod"},
        };

        private readonly Dictionary<string, string> NodeToMongoAggregationOperatorDict = new Dictionary<string, string> {
            {"Sum", "sum"},
            {"Min", "min"},
            {"Max", "max"},
            {"Average", "avg"},
            {"First", "first"},
            {"Last", "last"},
            {"Count", "sum"}
        };

        private readonly Dictionary<string, string> NodeToMongoDateOperatorDict  = new Dictionary<string, string> {
            {"Year", "$year"},
            {"Month", "$month"},
            {"Day", "$dayOfMonth"},
            {"Hour", "$hour"},
            {"Minute", "$minute"},
            {"Second", "$second"},
            {"Millisecond", "$millisecond"},
            {"DayOfWeek", "$dayOfWeek"},
            {"DayOfYear", "$dayOfYear"},
        };

        /// <summary>Constructs a new MongoPipeline from a typed MongoCollection</summary>
        public MongoPipeline(MongoCollection<TDocType> collection, bool allowMongoDiskUse, Action<string> loggingDelegate)
        {
            _loggingDelegate = loggingDelegate;
            _collection = collection;
            _allowMongoDiskUse = allowMongoDiskUse;
        }

        /// <summary>Log a string to the logging delegate</summary>
        private void LogLine(string s)
        {
            if (_loggingDelegate == null)
                return;

            _loggingDelegate(s + Environment.NewLine);
        }

        /// <summary>Log a string with format parameters to the logging delegate</summary>
        private void LogLine(string s, params object[] parameters)
        {
            LogLine(string.Format(s + Environment.NewLine, parameters));
        }

        /// <summary>Log a newline to the logging delegate</summary>
        private void LogLine()
        {
            LogLine(Environment.NewLine);
        }

        /// <summary>
        /// Adds a stage to our aggregation pipeline
        /// </summary>
        /// <param name="pipelineOperatorName">The operation name ($match, $group, etc)</param>
        /// <param name="operation">See MongoDB reference.  This is the operation to perform.</param>
        /// <param name="insertBefore">Pipeline stage to insert this stage before.  Defaults to the end.</param>
        public PipelineStage AddToPipeline(string pipelineOperatorName, BsonValue operation, int insertBefore = Int32.MaxValue)
        {
            var newStage = new PipelineStage {
                PipelineOperator = pipelineOperatorName,
                Operation = operation
            };

            _pipeline.Insert(Math.Min(insertBefore, _pipeline.Count()), newStage);
            return newStage;
        }

        /// <summary>Gets the Mongo field name that the specified MemberInfo maps to.</summary>
        private string GetMongoFieldName(MemberInfo member)
        {
            // Get the BsonElementAttribute that MIGHT be decorating the field/property we're accessing
            var bsonElementAttribute = (BsonElementAttribute) member.GetCustomAttributes(typeof(BsonElementAttribute), true).SingleOrDefault();
            if (bsonElementAttribute != null)
                return bsonElementAttribute.ElementName;

            // Get the BsonIdAttribute that MIGHT be decorating the field/property we're accessing
            var bsonIdAttribute = (BsonIdAttribute) member.GetCustomAttributes(typeof(BsonIdAttribute), true).SingleOrDefault();
            if (bsonIdAttribute != null)
                return "_id";

            // IGrouping.Key maps to to the "_id" resulting from a $group
            if (member.DeclaringType != null && member.DeclaringType.Name == "IGrouping`2")
                return "_id";

            // TODO BUGBUG (Issue #8): Do a better fix for htis
            if (member.Name == "Id")
                return "_id";

            // At this point, we should just use the member name
            return member.Name;
        }

        /// <summary>
        /// Gets the name of the Mongo Field that the specified MemberExpression maps to.
        /// </summary>
        private string GetMongoFieldName(Expression expression, bool isNamedProperty)
        {
            if (expression.NodeType == ExpressionType.Convert)
            {
                // A field that's an enum goes through a cast to an int.
                return GetMongoFieldName(((UnaryExpression) expression).Operand, isNamedProperty);
            }

            if (expression is MemberExpression)
            {
                isNamedProperty = true;
                var memberExp = (MemberExpression) expression;

                // We might have a nested MemberExpression like c.Key.Name
                // So recurse to build the whole mongo field name
                string prefix = "";
                if (memberExp.Expression is MemberExpression)
                {
                    prefix = GetMongoFieldName(memberExp.Expression, isNamedProperty) + '.';
                }
                else if (memberExp.Expression is MethodCallExpression)
                {
                    string fieldName = GetMongoFieldNameForMethodOnGrouping((MethodCallExpression) memberExp.Expression).AsString;

                    // Remove the '$'
                    prefix = (fieldName.StartsWith("$") ? fieldName.Substring(1) : fieldName) + '.';
                }

                return prefix + GetMongoFieldName(memberExp.Member);
            }

            if (expression.NodeType == ExpressionType.Parameter)
            {
                return isNamedProperty ? PIPELINE_DOCUMENT_RESULT_NAME : null;
            }

            if (expression.NodeType == ExpressionType.Call)
            {
                string fieldName = GetMongoFieldNameForMethodOnGrouping((MethodCallExpression) expression).AsString;

                // Remove the '$'
                return fieldName.StartsWith("$") ? fieldName.Substring(1) : fieldName;
            }

            throw new InvalidQueryException("Can't get Mongo field name for expression type " + expression.NodeType);
        }

        /// <summary>
        /// Emits a pipeline stage for a GroupBy operation.
        /// By default, this stage ONLY includes the grouping key and does not include
        /// any aggregations (ie summing of grouped documents) or the grouped documents
        /// themselves.  Later pipeline stages may edit the output of this pipeline stage
        /// to include other fields.
        /// </summary>
        public void EmitPipelineStageForGroupBy(LambdaExpression lambdaExp)
        {
            // GroupBy supports the following modes:
            //    ParameterExpression: GroupBy(c => c)
            //    NewExpression:       GroupBy(c => new { c.Age, Name = c.FirstName })
            //    Other expressions:   GroupBy(c => c.Age + 1)   

            // Handle the first case: GroupBy(c => c)
            if (lambdaExp.Body is ParameterExpression)
            {
                // This point was probably reached by doing something like:
                //   .Select(c => c.FirstName).GroupBy(c => c)

                // Perform the grouping on the _result_ document (which we'll assume we have)
                var pipelineOperation = new BsonDocument { new BsonElement("_id", "$" + PIPELINE_DOCUMENT_RESULT_NAME) };
                AddToPipeline("$group", pipelineOperation).GroupNeedsCleanup = true;
                return;
            }

            // Handle an anonymous type: GroupBy(c => new { c.Age, Name = c.FirstName })
            if (lambdaExp.Body is NewExpression)
            {
                var newExp = (NewExpression) lambdaExp.Body;
                var newExpProperties = newExp.Type.GetProperties();

                // Get the mongo field names for each property in the new {...}
                var fieldNames = newExp.Arguments
                    .Select((c, i) => new {
                        KeyFieldName = newExpProperties[i].Name,
                        ValueMongoExpression = BuildMongoSelectExpression(c)
                    })
                    .Select(c => new BsonElement(c.KeyFieldName, c.ValueMongoExpression));

                // Perform the grouping on the multi-part key
                var pipelineOperation = new BsonDocument {new BsonElement("_id", new BsonDocument(fieldNames))};
                AddToPipeline("$group", pipelineOperation).GroupNeedsCleanup = true;
                return;
            }                     

            // Handle all other expression types
            var bsonValueExpression = BuildMongoSelectExpression(lambdaExp.Body);

            // Perform the grouping
            AddToPipeline("$group", new BsonDocument {new BsonElement("_id", bsonValueExpression)}).GroupNeedsCleanup = true;
            return;
        }

        /// <summary>
        /// Gets a bson value from a constant.  Handles int, long, bool, and treats all other types as string
        /// </summary>
        private BsonValue GetBsonValueFromObject(object obj)
        {
            if (obj is int || obj is Enum)
                return new BsonInt32((int) obj);

            if (obj is long)
                return new BsonInt64((long) obj);

            if (obj is bool)
                return new BsonBoolean((bool) obj);

            if (obj is double)
                return new BsonDouble((double) obj);

            if (obj is DateTime)
                return new BsonDateTime((DateTime) obj);

            if (obj == null)
                return BsonNull.Value;

            if (obj is string)
                return new BsonString((string) obj);

            if (obj is Guid)
                return BsonValue.Create(obj);

            if (obj is ObjectId)
                return BsonValue.Create(obj);

            if (TypeSystem.FindIEnumerable(obj.GetType()) != null)
            {
                var bsonArray = new BsonArray();
                foreach (var element in (IEnumerable) obj)
                    bsonArray.Add(GetBsonValueFromObject(element));

                return bsonArray;
            }

            throw new InvalidQueryException("Can't convert type " + obj.GetType().Name + " to BsonValue");
        }

        /// <summary>
        /// Builds an IMongoQuery from a given expression for use in a $match stage.
        /// We build an IMongoQuery rather than a BsonValue simply as a shortcut.
        /// It's easier to build an IMongoQuery and then call .ToBsonDocument on the result.
        /// </summary>
        /// <param name="expression">Expression to build a query for</param>
        /// <param name="isLambdaParamResultHack">
        /// Required for proper query sematics in $elemmatch expressions.
        /// 
        /// Whether a reference to a lambda parameter in the expression should be treated as a reference
        /// to the field named PIPELINE_DOCUMENT_RESULT_NAME.  Generally this will be true.  This the case
        /// for something like this:
        /// .Select(c => c.Age)
        /// .Where(c => c > 15)
        /// In this case the variable 'c' is referring to the _result_ field in the document
        /// 
        /// Pass in false when dealing with a sub-predicate like this:
        /// .Where(c => c.SubArrayOfInts.Any(d => d == 15))
        /// In this case the variable 'd' is is referring to a sub-item in the nested array and
        /// requires different syntax of any $elemmatch expressions.
        /// 
        /// TODO: We should do a better job here entirely.  We currently don't do a good job
        /// of tracking our lambda params through the full Where call.  For example, this is undefined:
        /// .Where(c => c.SubArray.Any(d => d.Value == c.SomeOtherLocalProperty))
        /// </param>
        private IMongoQuery BuildMongoWhereExpressionAsQuery(Expression expression, bool isLambdaParamResultHack)
        {
            // Handle binary operators (&&, ==, >, etc)
            if (expression is BinaryExpression)
            {
                var binExp = (BinaryExpression) expression;

                // If the LHS is an array length, then we use special operators.
                if (binExp.Left.NodeType == ExpressionType.ArrayLength)
                {
                    // Mongo doesn't natively support .Where(c => c.array.Length != 2)
                    // So translate that to           .Where(c => !(c.array.Length == 2))
                    var localNodeType = binExp.NodeType == ExpressionType.NotEqual ? ExpressionType.Equal : binExp.NodeType;
                    bool invert = binExp.NodeType == ExpressionType.NotEqual;

                    // Validate it's a supported operator
                    if (!NodeToMongoQueryBuilderArrayLengthFuncDict.Keys.Contains(localNodeType))
                        throw new InvalidQueryException("Unsupported binary operator '" + binExp.NodeType + "' on Array.Length");

                    // Validate we're comparing to a const
                    if (binExp.Right.NodeType != ExpressionType.Constant)
                        throw new InvalidQueryException("Array.Length can only be compared against a constant");

                    // Retrieve the function (like Query.EQ) that we'll use to generate our mongo query
                    var queryFunc = NodeToMongoQueryBuilderArrayLengthFuncDict[localNodeType];

                    // Get our operands
                    int rhs = (int) ((ConstantExpression) binExp.Right).Value;
                    string mongoFieldName = GetMongoFieldName(((UnaryExpression) binExp.Left).Operand, isLambdaParamResultHack);
                    if (mongoFieldName == null)
                        throw new NotImplementedException("ExpressionType.ArrayLength on lambda parameters not supported. ie: .Where(c => c.SubArray.Any(d => d.Length > 5))");

                    // Generate the query
                    var query = queryFunc(mongoFieldName, rhs);

                    // Optionally invert our query
                    return invert ? Query.Not(query) : query;
                }

                // If this binary expression is in our expression node type -> Mongo query dict, then use it
                if (NodeToMongoQueryBuilderFuncDict.Keys.Contains(expression.NodeType))
                {
                    // The left side of the operator MUST be a mongo field name
                    string mongoFieldName = GetMongoFieldName(binExp.Left, isLambdaParamResultHack);

                    // Build the Mongo expression for the right side of the binary operator
                    BsonValue rightValue = BuildMongoWhereExpressionAsBsonValue(binExp.Right, isLambdaParamResultHack);

                    // Retrieve the function (like Query.EQ) that we'll use to generate our mongo query
                    var queryOperator = NodeToMongoQueryBuilderFuncDict[expression.NodeType];

                    // Generate the query and return it as a new BsonDocument
                    var queryDoc = new BsonDocument(queryOperator, rightValue);
                    if (mongoFieldName != null)
                        queryDoc = new BsonDocument(mongoFieldName, queryDoc);

                    return Query.Create(queryDoc);
                }

                // Handle && and ||
                if (expression.NodeType == ExpressionType.AndAlso || expression.NodeType == ExpressionType.OrElse)
                {
                    // Build the Mongo expression for the left side of the binary operator
                    var leftQuery = BuildMongoWhereExpressionAsQuery(binExp.Left, isLambdaParamResultHack);
                    var rightQuery = BuildMongoWhereExpressionAsQuery(binExp.Right, isLambdaParamResultHack);
                    return expression.NodeType == ExpressionType.AndAlso ? Query.And(leftQuery, rightQuery) : Query.Or(leftQuery, rightQuery);
                }
            }

            // Handle unary operator not (!)
            if (expression.NodeType == ExpressionType.Not)
            {
                var unExp = (UnaryExpression) expression;
                return Query.Not(BuildMongoWhereExpressionAsQuery(unExp.Operand, isLambdaParamResultHack));
            }

            // Handle .IsMale case in: .Where(c => c.IsMale || c.Age == 15)
            if (expression.NodeType == ExpressionType.MemberAccess)
            {
                return Query.EQ(GetMongoFieldName(expression, isLambdaParamResultHack), true);
            }

            // Handle method calls on sub properties
            // .Where(c => string.IsNullOrEmpty(c.Name))
            // .Where(c => c.Names.Contains("Bob"))
            // etc...
            if (expression.NodeType == ExpressionType.Call)
            {
                var callExp = (MethodCallExpression) expression;

                // Support .Where(c => c.ArrayProp.Contains(1)) and .Where(c => new[] { 1, 2, 3}.Contains(c.Id))
                if (callExp.Method.Name == "Contains")
                {
                    // Part 1 - Support .Where(c => someLocalEnumerable.Contains(c.Field))
                    // Extract the IEnumerable that .Contains is being called on
                    // Important to note that it can be in callExp.Object (for a List) or in callExp.Arguments[0] (for a constant, read-only array)
                    var arrayConstantExpression = (callExp.Object ?? callExp.Arguments[0]) as ConstantExpression;
                    if (arrayConstantExpression != null)
                    {
                        var localEnumerable = arrayConstantExpression.Value;
                        if (TypeSystem.FindIEnumerable(localEnumerable.GetType()) == null)
                        {
                            throw new InvalidQueryException("In Where(), Contains() only supported on IEnumerable");
                        }

                        // Get the field that we're going to search for within the IEnumerable
                        var mongoFieldName = GetMongoFieldName(callExp.Arguments.Last(), isLambdaParamResultHack);

                        // Evaluate the IEnumerable
                        var array = (BsonArray) GetBsonValueFromObject(localEnumerable);

                        if (mongoFieldName == null)
                            return Query.Create("$in", array);

                        return Query.In(mongoFieldName, array.AsEnumerable());
                    }

                    // Par 2 - Support .Where(c => c.SomeArrayProperty.Contains("foo"))
                    string searchTargetMongoFieldName = GetMongoFieldName(callExp.Arguments[0], isLambdaParamResultHack);
                    var searchItem = BsonValue.Create(((ConstantExpression) callExp.Arguments[1]).Value);
                    return Query.All(searchTargetMongoFieldName, new[] {searchItem});
                }

                // Support .Where(c => string.IsNullOrEmpty(c.Name))
                if (callExp.Method.Name == "IsNullOrEmpty" && callExp.Object == null && callExp.Method.ReflectedType == typeof(string))
                {
                    var mongoFieldName = GetMongoFieldName(callExp.Arguments.Single(), isLambdaParamResultHack);
                    return Query.Or(Query.EQ(mongoFieldName, BsonNull.Value), Query.EQ(mongoFieldName, new BsonString("")));
                }

                // Support .Where(c => c.SomeArrayProp.Any()) and .Where(c => c.SomeArrayProp.Any(d => d.SubProp > 5))
                if (callExp.Method.Name == "Any")
                {
                    // Support .Where(c => c.SomeArrayProp.Any())
                    if (callExp.Arguments.Count() == 1)
                    {
                        throw new NotImplementedException("TODO: Implement .Where(c => c.SomeArrayProp.Any())");
                    }

                    // Support .Where(c => c.SomeArrayProp.Any(d => d.SubProp > 5))
                    var mongoFieldName = GetMongoFieldName(callExp.Arguments[0], isLambdaParamResultHack);
                    var predicateExpression = (LambdaExpression) callExp.Arguments[1];
                    
                    var query = Query.ElemMatch(mongoFieldName, BuildMongoWhereExpressionAsQuery(predicateExpression.Body, false));
                    return query;
                }

                throw new InvalidQueryException("No translation for method " + callExp.Method.Name);
            }

            if (expression is ConstantExpression)
            {
                // This is for handling .Where(c => true) and .Where(c => false).
                // We can use Query.NotExists to achieve this
                bool expressionValue = (bool) ((ConstantExpression) expression).Value;
                return expressionValue ? Query.NotExists("_this_field_does_not_exist_912419254012") : Query.Exists("_this_field_does_not_exist_912419254012");
            }

            throw new InvalidQueryException("In Where(), can't build Mongo expression for node type" + expression.NodeType);
        }

        /// <summary>
        /// Gets the lambda (which may be null) from a MethodCallExpression 
        /// </summary>
        /// <returns>A LambdaExpression, possibly null</returns>
        private static LambdaExpression GetLambda(MethodCallExpression mce)
        {
            if (mce.Arguments.Count < 2)
                return null;

            return (LambdaExpression) ((UnaryExpression) mce.Arguments[1]).Operand;
        }


        /// <summary>
        /// Builds a Mongo expression for use in a $match statement from a given expression.
        /// See documentation of BuildMongoWhereExpressionAsQuery for the explanation around isLambdaParamResultHack
        /// </summary>
        private BsonValue BuildMongoWhereExpressionAsBsonValue(Expression expression, bool isLambdaParamResultHack)
        {
            if (expression is MemberExpression)
            {
                throw new InvalidQueryException("Can't use field name on right hand side of expression.");

                // It would sure be nice if we could do this.
                // Except Mongo doesn't support this yet: .Where(c => c.Field1 == c.Field2).
                // return new BsonString("$" + GetMongoFieldName(expression));
            }

            if (expression is ConstantExpression)
            {
                return GetBsonValueFromObject(((ConstantExpression) expression).Value);
            }

            return BuildMongoWhereExpressionAsQuery(expression, isLambdaParamResultHack).ToBsonDocument();
        }

        /// <summary>
        /// Allow an aggregation to be run on each group of a grouping.
        /// Examples:
        ///     .GroupBy(...).Where(c => c.Count() > 1)
        ///     .GroupBy(...).Select(c => c.Sum())
        /// This function modifies the previous .GroupBy ($group) pipeline operation
        /// by including this aggregation and then returns the fieldname containing
        /// the result of the aggregation. 
        /// </summary>
        /// <param name="callExp">The MethodCallExpression being run on the grouping</param>
        /// <returns>The mongo field name</returns>
        public BsonString GetMongoFieldNameForMethodOnGrouping(MethodCallExpression callExp)
        {
            // Only allow a function within a Select to be called on a group
            if (callExp.Arguments[0].Type.Name != "IGrouping`2")
                throw new InvalidQueryException("Aggregation \"" + callExp.Method.Name + "\"can only be run after a GroupBy");

            // Get the $group document from the most recent $group pipeline stage
            var groupDoc = GetLastOccurrenceOfPipelineStage("$group", false);

            // Handle aggregation functions within the select (Sum, Max, etc)
            //    .Select(c => c.Sum(d => d.Age))
            // Would get converted converted to this Bson for use in the project:
            //    $sum0
            // Then this element would be added to the prio $group pipeline stage:
            //    {$sum:"$age"}
            if (!NodeToMongoAggregationOperatorDict.ContainsKey(callExp.Method.Name))
                throw new InvalidQueryException("Method " + callExp.Method.Name + " not supported on Grouping");

            // Get the mongo operator (ie "$sum") that this method maps to
            string mongoOperator = NodeToMongoAggregationOperatorDict[callExp.Method.Name];

            // Create a temporary variable name for using in our project statement
            // This will look like "sum0" or "avg1"
            string tempVariableName = mongoOperator + _nextUniqueVariableId++;

            // Get the operand for the operator
            BsonValue mongoOperand;
            if (callExp.Method.Name == "Count" || callExp.Method.Name == "First" || callExp.Method.Name == "Last")
            {
                // We don't support a lambda within the .Count
                // No good:   .Select(d => d.Count(e => e.Age > 15))
                if (callExp.Arguments.Count > 1)
                    throw new InvalidQueryException("Argument within " + callExp.Method.Name + " within Select not supported");

                // If we're counting, then the expression is {$count: 1}
                if (callExp.Method.Name == "Count")
                    mongoOperand = new BsonInt32(1);
                else
                {
                    // For First and Last, the expression is {$first: "$$ROOT"} or {$last: "$$ROOT"}
                    mongoOperand = new BsonString("$$ROOT");
                }
            }
            else if (callExp.Arguments.Count == 2)
            {
                // Get the inner lambda; "d => d.Age" from the above example
                var lambdaExp = (LambdaExpression) callExp.Arguments[1];

                // Get the operand for the operator
                mongoOperand = BuildMongoSelectExpression(lambdaExp.Body);
            }
            else
            {
                throw new InvalidQueryException("Unsupported usage of " + callExp.Method.Name + " within Select");
            }

            // Build the expression being aggregated
            var aggregationDoc = new BsonDocument("$" + mongoOperator, mongoOperand);

            // Add to the $group stage, a new variable which receives our aggregation
            var newGroupElement = new BsonDocument(tempVariableName, aggregationDoc);
            groupDoc.AddRange(newGroupElement);

            // Return our temp variable as the field name to use in this projection
            return new BsonString("$" + tempVariableName);
        }

        /// <summary>
        /// Builds a Mongo expression for use in a $project statement from a given expression 
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="specialTreatmentForConst"></param>
        /// <returns></returns>
        public BsonValue BuildMongoSelectExpression(Expression expression, bool specialTreatmentForConst = false)
        {
            // TODO: What about enums here?

            // c.Age
            if (expression is MemberExpression)
            {
                // Handle member access of DateTime objects
                var memberExpression = (MemberExpression) expression;
                if (memberExpression.Member.DeclaringType == typeof(string))
                {
                    if (memberExpression.Member.Name == "Length")
                        return new BsonDocument("$strLenCP", BuildMongoSelectExpression(memberExpression.Expression));

                    throw new InvalidQueryException($"{memberExpression.Member.Name} property on String not supported due to lack of Mongo support :(");
                }

                if (memberExpression.Member.DeclaringType == typeof(DateTime))
                {
                    if (memberExpression.Member.Name == "Date")
                    {
                        // No support for Date but we can hack it by subtracting out the time component (in milliseconds)
                        var hours = new BsonDocument("$hour", BuildMongoSelectExpression(memberExpression.Expression));
                        var minutes = new BsonDocument("$minute", BuildMongoSelectExpression(memberExpression.Expression));
                        var seconds = new BsonDocument("$second", BuildMongoSelectExpression(memberExpression.Expression));
                        var millis = new BsonDocument("$millisecond", BuildMongoSelectExpression(memberExpression.Expression));
                        var hoursAsMillis = new BsonDocument("$multiply", new BsonArray(new[] { hours, (BsonValue) 3600000}));
                        var minutesAsMillis = new BsonDocument("$multiply", new BsonArray(new[] { minutes, (BsonValue) 60000}));
                        var secondsAsMillis = new BsonDocument("$multiply", new BsonArray(new[] { seconds, (BsonValue) 1000}));
                        var totalMillis = new BsonDocument("$add", new BsonArray(new[] { hoursAsMillis, minutesAsMillis, secondsAsMillis, millis }));
                        var array = new BsonArray(new[] { BuildMongoSelectExpression(memberExpression.Expression), totalMillis});
                        return new BsonDocument("$subtract", array);
                    }

                    // .Net DayOfWeek is 0 indexed, Mongo is 1 indexed
                    if (memberExpression.Member.Name == "DayOfWeek")
                    {
                        var array = new BsonArray(new[] { new BsonDocument("$dayOfWeek", BuildMongoSelectExpression(memberExpression.Expression)), (BsonValue) 1});
                        return new BsonDocument("$subtract", array);
                    }

                    string mongoDateOperator;
                    if (NodeToMongoDateOperatorDict.TryGetValue(memberExpression.Member.Name, out mongoDateOperator))
                        return new BsonDocument(mongoDateOperator, BuildMongoSelectExpression(memberExpression.Expression));

                    throw new InvalidQueryException($"{memberExpression.Member.Name} property on DateTime not supported due to lack of Mongo support :(");
                }

                return new BsonString("$" + GetMongoFieldName(expression, true));
            }

            // 15
            if (expression is ConstantExpression)
            {
                var constExp = (ConstantExpression) expression;

                // Handle a special case:
                //     .Select(c => 15)
                // This would naturally tranlate to:
                //     {$project, {_result_:15}}
                // But that's not valid Mongo.  Instead, we need to build the 15 arithmetically:
                //     {$project, {_result:{$add:[0,15]}}}
                if (specialTreatmentForConst)
                {
                    if (constExp.Type == typeof (int))
                    {
                        // Build a binary-add expression that evaluates our 0 or 1
                        return BuildMongoSelectExpression(Expression.MakeBinary(ExpressionType.Add, Expression.Constant(0), constExp));
                    }

                    if (constExp.Type == typeof (string))
                    {
                        // Build an expression concatenating the string with nothing else
                        return new BsonDocument("$concat", new BsonArray(new[] {new BsonString((string) constExp.Value)}));
                    }

                    if (constExp.Type == typeof (bool))
                    {
                        return BuildMongoSelectExpression(Expression.MakeBinary(ExpressionType.OrElse, Expression.Constant(false), constExp));
                    }
                }
                return GetBsonValueFromObject((constExp).Value);
            }

            // c.Age + 10
            if (expression is BinaryExpression && NodeToMongoBinaryOperatorDict.ContainsKey(expression.NodeType))
            {
                var binExp = (BinaryExpression) expression;

                BsonValue leftValue = BuildMongoSelectExpression(binExp.Left);
                BsonValue rightValue = BuildMongoSelectExpression(binExp.Right);

                var array = new BsonArray(new[] {leftValue, rightValue});

                string mongoOperator = NodeToMongoBinaryOperatorDict[expression.NodeType];
                return new BsonDocument(mongoOperator, array);
            }

            // !c.IsMale
            if (expression.NodeType == ExpressionType.Not)
            {
                var unExp = (UnaryExpression) expression;
                BsonValue operandValue = BuildMongoSelectExpression(unExp.Operand);
                return new BsonDocument("$not", operandValue);
            }

            // c.IsMale ? "Man" : "Woman"
            if (expression.NodeType == ExpressionType.Conditional)
            {
                // Build expression with the Mongo $cond operator
                var condExp = (ConditionalExpression) expression;
                BsonValue testValue = BuildMongoSelectExpression(condExp.Test);
                BsonValue ifTrueValue = BuildMongoSelectExpression(condExp.IfTrue);
                BsonValue ifFalseValue = BuildMongoSelectExpression(condExp.IfFalse);
                var condDocValue = new BsonDocument(new[] {
                    new BsonElement("if", testValue),
                    new BsonElement("then", ifTrueValue),
                    new BsonElement("else", ifFalseValue)
                }.AsEnumerable());
                return new BsonDocument("$cond", condDocValue);
            }

            // string.IsNullOrEmpty
            if (expression.NodeType == ExpressionType.Call)
            {
                var callExp = (MethodCallExpression) expression;
                if (callExp.Method.Name == "IsNullOrEmpty" && callExp.Object == null && callExp.Method.ReflectedType == typeof (string))
                {
                   BsonValue expressionToTest = BuildMongoSelectExpression(callExp.Arguments.Single());

                    // Test for null
                    var ifNullDoc = new BsonDocument("$ifNull", new BsonArray(new[] {expressionToTest, new BsonString("")}));

                    // Test for empty
                    return new BsonDocument("$eq", new BsonArray(new[] {new BsonString(""), ifNullDoc.AsBsonValue}));
                }

                if (callExp.Type == typeof(string))
                {
                    if (callExp.Method.Name == "ToUpper")
                        return new BsonDocument("$toUpper", BuildMongoSelectExpression(callExp.Object));
                    if (callExp.Method.Name == "ToLower")
                        return new BsonDocument("$toLower", BuildMongoSelectExpression(callExp.Object));

                    throw new InvalidQueryException($"Can't translate method {callExp.Type.Name}.{callExp.Method.Name} to Mongo expression");
                }

                // c.Contains (where c is an Enumerable)
                if (callExp.Method.Name == "Contains" && callExp.Method.ReflectedType == typeof (Enumerable))
                {
                    var searchValue = BuildMongoSelectExpression(callExp.Arguments[1]);
                    var searchTarget = BuildMongoSelectExpression(callExp.Arguments[0]);
                    
                    return new BsonDocument("$in", new BsonArray(new[] { searchValue, searchTarget }) );
                }

                // c.Sum(d => d.Age), c.Count(), etc
                return GetMongoFieldNameForMethodOnGrouping(callExp);
            }

            // Casts
            if (expression.NodeType == ExpressionType.Convert)
            {
                var unExp = (UnaryExpression) expression;
                return BuildMongoSelectExpression(unExp.Operand);
            }

            // Array.Length
            if (expression.NodeType == ExpressionType.ArrayLength)
            {
                var arrayLenExp = (UnaryExpression) expression;
                return new BsonDocument("$size", BuildMongoSelectExpression(arrayLenExp.Operand));
            }

            throw new InvalidQueryException("In Select(), can't build Mongo expression for node type" + expression.NodeType);
        }

        /// <summary>
        /// Gets the last occurrence of pipeline operation in the pipeline for a given operator.
        /// </summary>
        /// <param name="pipelineOperator">The stage to find (example: "$group")</param>
        /// <param name="mustBeLastStageInPipeline">
        /// If true, then the operator must be the LAST stage in the pipeline.  If false, then simply the last occurrence
        /// of the operator in the pipeline is found.
        /// </param>
        /// <returns>The pipeline operation being performed (not including the pipeline operator itself).  So if the search
        /// was for "$sort" and this stage was found, "{$sort, {age:1}}", then just "{age:1}" is returned.
        /// Null is returned if the stage couldn't be found.</returns>
        public BsonDocument GetLastOccurrenceOfPipelineStage(string pipelineOperator, bool mustBeLastStageInPipeline)
        {
            if (mustBeLastStageInPipeline)
            {
                var stage = _pipeline.Last();
                return stage.PipelineOperator == pipelineOperator ? (BsonDocument) stage.Operation : null;
            }
            else
            {
                var stage = _pipeline.AsEnumerable().LastOrDefault(c => c.PipelineOperator == pipelineOperator);
                return stage != null ? (BsonDocument) stage.Operation : null;
            }
        }

        /// <summary>Adds a new $project stage to the pipeline for a .Select method call</summary>
        public void EmitPipelineStageForSelect(LambdaExpression lambdaExp)
        {
            // Select supports the following modes:
            //    NewExpression:      Select(c => new { c.Age + 10, Name = c.FirstName })
            //    Non-new expression: Select(c => 10)
            //    Non-new expression: Select(c => c.Age)
            //    Non-new expression: Select(c => (c.Age + 10) > 15)

            // Handle the hard case: Select(c => new { c.Age, Name = c.FirstName,  })
            if (lambdaExp.Body is NewExpression)
            {
                var newExp = (NewExpression) lambdaExp.Body;
                var newExpProperties = newExp.Type.GetProperties();

                // Get the mongo field names for each property in the new {...}
                var fieldNames = newExp.Arguments
                                       .Select((c, i) => new {
                                           FieldName = newExpProperties[i].Name,
                                           ExpressionValue = BuildMongoSelectExpression(c, true)
                                       })
                                       .Select(c => new BsonElement(c.FieldName, c.ExpressionValue))
                                       .ToList();

                // Remove the unnecessary _id field
                if (fieldNames.All(c => c.Name != "_id"))
                    fieldNames.Add(new BsonElement("_id", new BsonInt32(0)));

                // Perform the projection on multiple fields
                AddToPipeline("$project", new BsonDocument(fieldNames));
                return;
            }

            // Handle type typed hard case: Select(c => new Foo { Bar = c.FirstName })
            if (lambdaExp.Body is MemberInitExpression)
            {
                var memberInitExp = (MemberInitExpression) lambdaExp.Body;
                var fieldNames = memberInitExp.Bindings
                                              .Cast<MemberAssignment>()
                                              .Select(c => new {
                                                  FieldName = GetMongoFieldName(c.Member),
                                                  ExpressionValue = BuildMongoSelectExpression(c.Expression, true)
                                              })
                                              .Select(c => new BsonElement(c.FieldName, c.ExpressionValue))
                                              .ToList();

                // Remove the unnecessary _id field
                if (fieldNames.All(c => c.Name != "_id"))
                    fieldNames.Add(new BsonElement("_id", new BsonInt32(0)));

                // Perform the projection on multiple fields
                AddToPipeline("$project", new BsonDocument(fieldNames));
                return;
            }

            // Handle the simple non-new expression case: .Select(c => c.Age + 15)
            BsonValue expressionValue = BuildMongoSelectExpression(lambdaExp.Body, true);
            AddToPipeline("$project", new BsonDocument {
                new BsonElement(PIPELINE_DOCUMENT_RESULT_NAME, expressionValue),
                new BsonElement("_id", new BsonInt32(0)),
            });
        }

        /// <summary>Adds a new $match stage to the pipeline for a .Where method call</summary>
        public void EmitPipelinesStageForWhere(LambdaExpression lambdaExp)
        {
            if (lambdaExp == null)
                return;

            // Special case, handle .Where(c => true) and .Where(c => false)
            if (lambdaExp.Body is ConstantExpression)
            {
                var constExp = (ConstantExpression) lambdaExp.Body;

                // No-op if this lambda is .Where(c => true)
                if ((bool) constExp.Value)
                    return;
            }

            AddToPipeline("$match", BuildMongoWhereExpressionAsQuery(lambdaExp.Body, true).ToBsonDocument());
        }

        /// <summary>Adds a new $limit stage to the pipeline for a .Take method call</summary>
        public void EmitPipelineStageForTake(int limit)
        {
            AddToPipeline("$limit", new BsonInt32(limit));
        }

        /// <summary>Adds a new $limit stage to the pipeline for a .Take method call</summary>
        public void EmitPipelineStageForSkip(int limit)
        {
            AddToPipeline("$skip", new BsonInt32(limit));
        }

        /// <summary>Adds a new $project stage to the pipeline for a .Count method call</summary>
        public void EmitPipelineStageForCount(LambdaExpression lambdaExp)
        {
            // Handle 2 cases:
            //    .Count()
            //    .Count(c => c.Age > 25)

            // The hard case: .Count(c => c.Age > 25)
            // Can be rewritten as .Where(c => c.Age > 25).Count()
            if (lambdaExp != null)
            {
                EmitPipelinesStageForWhere(lambdaExp);
            }

            // Handle the simple case: .Count()
            AddToPipeline("$group", new BsonDocument {
                {"_id", new BsonDocument()},
                {PIPELINE_DOCUMENT_RESULT_NAME, new BsonDocument("$sum", 1)}
            });
        }

        /// <summary>Adds a new $project stage to the pipeline for a .Any method call</summary>
        public void EmitPipelineStageForAny(LambdaExpression lambdaExp)
        {
            // Handle 2 cases:
            //    .Count()
            //    .Count(c => c.Age > 25)

            // The hard case: .Any(c => c.Age > 25)
            // Can be rewritten as .Where(c => c.Age > 25).Any()
            if (lambdaExp != null)
            {
                EmitPipelinesStageForWhere(lambdaExp);
            }

            // Handle the simple case: .Any()

            // There is no explicity support for this in MongoDB, but we can
            // limit our results to 1 and then do a count
            EmitPipelineStageForTake(1);
            EmitPipelineStageForCount(null);

            // After executing the pipeline, we then check that the count > 0
        }

        /// <summary>Adds a pipeline stage ($group) for the specified aggregation (Sum, Min, Max, Average)</summary>
        public void EmitPipelineStageForAggregation(string cSharpAggregationName, LambdaExpression lambdaExp)
        {
            string mongoAggregationName = "$" + NodeToMongoAggregationOperatorDict[cSharpAggregationName];

            // Handle 2 cases:
            //    .Sum()
            //    .Sum(c => c.NumPets + 1)

            BsonValue bsonValue = lambdaExp == null ? "$" + PIPELINE_DOCUMENT_RESULT_NAME : BuildMongoSelectExpression(lambdaExp.Body);

            AddToPipeline("$group", new BsonDocument {
                {"_id", new BsonDocument()},
                {PIPELINE_DOCUMENT_RESULT_NAME, new BsonDocument(mongoAggregationName, bsonValue)}
            });
        }

        /// <summary>Adds a pipeline stage ($sort) for OrderBy and OrderByDescending</summary>
        public void EmitPipelineStageForOrderBy(LambdaExpression lambdaExp, bool ascending)
        {
            string field = GetMongoFieldName(lambdaExp.Body, true);
            AddToPipeline("$sort", new BsonDocument(field, ascending ? 1 : -1));
        }

        /// <summary>Updates the previous $sort pipeline stage for the ThenBy cal</summary>
        public void EmitPipelineStageForThenBy(LambdaExpression lambdaExp, bool ascending)
        {
            var sortDoc = GetLastOccurrenceOfPipelineStage("$sort", false);
            string field = GetMongoFieldName(lambdaExp.Body, true);
            if (sortDoc.Contains(field))
                throw new InvalidQueryException("ThenBy(Descending) can't resort on previous sort field.");

            sortDoc.Add(field, ascending ? 1 : -1);
        }

        /// <summary>Adds the respective pipeline stage(s) for the supplied method call</summary>
        public void EmitPipelineStageForMethod(MethodCallExpression expression)
        {
            // Our pipeline is a series of chained MethodCallExpression
            //         var results = queryable.Where(c => c.Age > 15).OrderBy(c => c.FirstName).Take(5)
            //
            // This gets compiled into an expression tree like:
            //         Take(OrderBy(Where(c => operator_GreaterThan(c.Age, 15)), c => c.FirstName), 5)
            //
            // And we ultimately want to emit these method calls as a series of MongoDB pipeline operations
            //         { "$match" : { "age" : { "$gt" : 15 } } }
            //         { "$sort" : {"age: 1"} }
            //         { "$limit" : 5 }

            // First recursively process the earlier method call in the method chain
            // That is, in blahblahblah.Where(...).Take(5), recursively process the .Where before handling the .Take
            if (expression.Arguments[0] is MethodCallExpression)
            {
                EmitPipelineStageForMethod((MethodCallExpression) expression.Arguments[0]);
            }

            switch (expression.Method.Name)
            {
                case "Where":
                {
                    EmitPipelinesStageForWhere(GetLambda(expression));
                    return;
                }
                case "Take":
                case "Skip":
                {
                    int numToTakeOrSkip = (int) ((ConstantExpression) expression.Arguments[1]).Value;
                    if (expression.Method.Name == "Take")
                        EmitPipelineStageForTake(numToTakeOrSkip);
                    else
                        EmitPipelineStageForSkip(numToTakeOrSkip);
                    return;
                }
                case "GroupBy":
                {
                    EmitPipelineStageForGroupBy(GetLambda(expression));
                    _lastPipelineOperation = _lastPipelineOperation | PipelineResultType.Grouped;
                    return;
                }
                case "Select":
                {
                    EmitPipelineStageForSelect(GetLambda(expression));
                    _lastPipelineOperation = PipelineResultType.Enumerable;
                    return;
                }
                case "OrderBy":
                    EmitPipelineStageForOrderBy(GetLambda(expression), true);
                    return;
                case "OrderByDescending":
                    EmitPipelineStageForOrderBy(GetLambda(expression), false);
                    return;
                case "ThenBy":
                    EmitPipelineStageForThenBy(GetLambda(expression), true);
                    return;
                case "ThenByDescending":
                    EmitPipelineStageForThenBy(GetLambda(expression), false);
                    return;
                case "Count":
                    EmitPipelineStageForCount(GetLambda(expression));
                    _lastPipelineOperation = PipelineResultType.Aggregation;
                    return;
                case "Any":
                    EmitPipelineStageForAny(GetLambda(expression));
                    _lastPipelineOperation = PipelineResultType.Any;
                    return;
                case "Sum":
                case "Max":
                case "Min":
                case "Average":
                    EmitPipelineStageForAggregation(expression.Method.Name, GetLambda(expression));
                    _lastPipelineOperation = PipelineResultType.Aggregation;
                    return;
                case "First":
                    EmitPipelinesStageForWhere(GetLambda(expression));
                    EmitPipelineStageForTake(1);
                    _lastPipelineOperation = _lastPipelineOperation | PipelineResultType.OneResultFromEnumerable | PipelineResultType.First;
                    return;
                case "FirstOrDefault":
                    EmitPipelinesStageForWhere(GetLambda(expression));
                    EmitPipelineStageForTake(1);
                    _lastPipelineOperation = _lastPipelineOperation | PipelineResultType.OneResultFromEnumerable | PipelineResultType.First | PipelineResultType.OrDefault;
                    return;
                case "Single":
                    EmitPipelinesStageForWhere(GetLambda(expression));
                    EmitPipelineStageForTake(2);
                    _lastPipelineOperation = _lastPipelineOperation | PipelineResultType.OneResultFromEnumerable | PipelineResultType.Single;
                    return;
                case "SingleOrDefault":
                    EmitPipelinesStageForWhere(GetLambda(expression));
                    EmitPipelineStageForTake(2);
                    _lastPipelineOperation = _lastPipelineOperation | PipelineResultType.OneResultFromEnumerable | PipelineResultType.Single | PipelineResultType.OrDefault;
                    return;
            }

            throw new InvalidQueryException("Unsupported MethodCallExpression " + expression.Method.Name);
        }
        
        /// <summary>
        /// Build and execute (ie evaluate) the supplied expression on a MongoDB server
        /// </summary>
        /// <typeparam name="TResult">The type of the query result</typeparam>
        /// <param name="expression">The query/expression to build and execute</param>
        public TResult Execute<TResult>(Expression expression)
        {
            // Build the pipeline

            if (expression is MethodCallExpression)
            {
                var methodExpression = (MethodCallExpression) expression;

                // queryable.Count() via aggregation framework is slow.  Handle that case specifically by asking the collection itself.
                if (methodExpression.Method.Name == "Count"
                    && methodExpression.Arguments.Count == 1
                    && methodExpression.Arguments[0] is ConstantExpression)
                {
                    // Todo: Any way to avoid the boxing?
                    return (TResult) (object) (int) _collection.Count();
                }

                EmitPipelineStageForMethod(methodExpression);
            }

            // If the result is a grouping, then we need to also include the values (ie not just the Key) in the result
            if ((_lastPipelineOperation & PipelineResultType.Grouped) != 0)
            {
                var groupDoc = GetLastOccurrenceOfPipelineStage("$group", false);
                groupDoc.Add("Values", new BsonDocument("$push", "$$ROOT"));
            }

            // Run our actual aggregation command against mongo
            var pipelineStages = _pipeline.Select(c => new BsonDocument(c.PipelineOperator, c.Operation)).ToArray();

            LogLine("\r\n----------------- PIPELINE --------------------\r\n");
            LogLine(string.Join("\r\n", pipelineStages.Select(c => c.ToString())));
            LogLine();

            var commandResult = _collection.Aggregate(new AggregateArgs {
                OutputMode = AggregateOutputMode.Cursor,
                AllowDiskUse = _allowMongoDiskUse,
                Pipeline = pipelineStages
            });

            // Handle aggregated result types
            if ((_lastPipelineOperation & PipelineResultType.Aggregation) != 0)
            {
                var results = commandResult.Take(2).ToArray();

                if (results.Length == 0)
                    return default(TResult);

                if (results.Length > 1)
                    throw new MongoLinqPlusPlusInternalExpception(string.Format("Unexpected number of results ({0}) for PipelineResultType.Aggregation pipeline", results.Length));

                // The result is in a document structured as { _result_ : value }
                var resultDoc = results[0];

                // Special treatment for any
                if (_lastPipelineOperation == PipelineResultType.Any)
                {
                    bool any = ((BsonInt32) resultDoc[PIPELINE_DOCUMENT_RESULT_NAME]).Value == 1;

                    // Todo: Any way to avoid the boxing (since we know TResult is bool)?
                    return (TResult) (object) any;
                }
                
                var aggregationResult = BsonSerializer.Deserialize<PipelineDocument<TResult>>(resultDoc);
                return aggregationResult._result_;
            }
            
            Type resultType = typeof(TResult);
            bool isGenericEnumerable = typeof(TResult) == typeof(IEnumerable);

            // Get the type that is in our enumerable result.
            // If we have plaine ole IEnumerable, then get it from the expression.
            // If we have an IEnumerable<T>, then get it from the generic type arguments of our result.
            // If we're runing a First, FirstOrDefault, Single, or SingleOrDefault, get it from resultType
            Type enumerableOfItemType;
            if (isGenericEnumerable)
                enumerableOfItemType = expression.Type.GenericTypeArguments[0];
            else if ((_lastPipelineOperation & PipelineResultType.OneResultFromEnumerable) != 0)
                enumerableOfItemType = resultType;
            else
                enumerableOfItemType = resultType.GenericTypeArguments[0];

            // Instantiate a List<enumerableOfItemType>
            Type listType = typeof (List<>).MakeGenericType(enumerableOfItemType);
            object list = Activator.CreateInstance(listType);
            var listAddMethod = listType.GetMethod("Add");
            object[] listAddMethodParameters = new object[1];

            bool firstIteration = true;
            Type simplePipelineDocType = null;
            PropertyInfo simplePipelineDocTypeResultProperty = null;

            bool enumerableOfItemTypeIsAnonymous = enumerableOfItemType.IsAnonymousType();
            TResult firstResult = default(TResult);

            int numResults = 0;

            foreach (var resultDoc in commandResult)
            {
                // See if we have an array of simple result types
                // [{ _result_: 5}, { _result_: 2}, ...]
                if (firstIteration && resultDoc.ElementCount == 1 && resultDoc.Contains(PIPELINE_DOCUMENT_RESULT_NAME))
                {
                    // We can't deserialize a BsonValue if it's not a document.
                    // So we need to deserialize using out simple result doc type.
                    simplePipelineDocType = typeof(PipelineDocument<>).MakeGenericType(enumerableOfItemType);
                    simplePipelineDocTypeResultProperty = simplePipelineDocType.GetProperty(PIPELINE_DOCUMENT_RESULT_NAME);
                }

                object deserializedResultItem;

                if (enumerableOfItemTypeIsAnonymous || ((_lastPipelineOperation & PipelineResultType.Grouped) != 0))
                {
                    // BsonSerializer can't handle anonymous types or IGrouping, so use Json.net

                    // We might have a simple doc type.  If so, extract our real result doc from _result_
                    var resultDocLocal = simplePipelineDocType == null ? resultDoc : (BsonDocument) resultDoc[PIPELINE_DOCUMENT_RESULT_NAME];

                    // Use Json.net for anonymous types.
                    string json = resultDocLocal.ToJson(_jsonWriterSettings);
                    deserializedResultItem = Newtonsoft.Json.JsonConvert.DeserializeObject(json, enumerableOfItemType, _customConverters);
                }
                else if (simplePipelineDocType != null)
                {
                    // Deserialize to a PipelineDocument using the BsonSerializer            
                    var pipelineDocument = BsonSerializer.Deserialize(resultDoc, simplePipelineDocType);

                    // Extract the result from the _result_ property
                    deserializedResultItem = simplePipelineDocTypeResultProperty.GetValue(pipelineDocument);
                }
                else
                {
                    // Easy case, just use the BsonSerializer
                    deserializedResultItem = BsonSerializer.Deserialize(resultDoc, enumerableOfItemType);
                }

                // Success for .First and .FirstOrDefault
                if ((_lastPipelineOperation & PipelineResultType.OneResultFromEnumerable) != 0)
                {
                    firstResult = (TResult) deserializedResultItem;
                    if ((_lastPipelineOperation & PipelineResultType.First) != 0)
                    {
                        return firstResult;
                    }
                }
                else
                {
                    // Call list.Add(deserializedResultItem);
                    listAddMethodParameters[0] = deserializedResultItem;
                    listAddMethod.Invoke(list, listAddMethodParameters);
                }

                numResults++;

                firstIteration = false;
            }

            // Handle .First, .Single, .FirstOrDefault, and .SingleOrDefault
            if ((_lastPipelineOperation & PipelineResultType.OneResultFromEnumerable) != 0)
            {
                if (numResults == 0)
                {
                    if ((_lastPipelineOperation & PipelineResultType.OrDefault) != 0)
                        return default(TResult);

                    throw new InvalidOperationException("Sequence contains no elements");
                }

                // First and first or default already returned results in our above loop

                // Blow up for more than one result
                if (numResults > 1)
                    throw new InvalidOperationException("Sequence contains more than one element");

                return firstResult;
            }

            return (TResult) list;
        }
    }
}
