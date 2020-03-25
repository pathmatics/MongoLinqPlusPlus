using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoLinqPlusPlus
{
    public static class MongoFunctions
    {
        /// <summary>
        /// C# doesn't support an expression like
        ///     c => c.String > "foo"
        /// where as MongoDB DOES support this method.
        /// This method translates to the Mongo equivalent of:
        ///     left > right
        ///
        /// This method is powerful because it allows us to write
        /// a range query against an indexed string column.
        /// </summary>
        public static bool GreaterThan(string left, string right) => string.CompareOrdinal(left, right) > 0;

        /// <summary>
        /// C# doesn't support an expression like
        ///     c => c.String >= "foo"
        /// where as MongoDB DOES support this method.
        /// This method translates to the Mongo equivalent of:
        ///     left >= right
        /// 
        /// This method is powerful because it allows us to write
        /// a range query against an indexed string column.
        /// </summary>
        public static bool GreaterThanOrEqual(string left, string right) => string.CompareOrdinal(left, right) >= 0;
    }
}
