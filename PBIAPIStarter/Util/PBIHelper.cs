using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

namespace PBIAPIStarter.Util
{
    public static class PBIHelper
    {

        /// <summary>
        /// Convert the table schema into Json string
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="name"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public static string ToJson(this SqlConnection sqlConnection, string name, string tablename)
        {
            StringBuilder schemaBuilder = new StringBuilder();

            string viewName = tablename.Substring(tablename.IndexOf(".") + 1);

            schemaBuilder.Append(string.Format("{0}\"name\": \"{1}\",\"tables\": [", "{", name));
            schemaBuilder.Append(String.Format("{0}\"name\": \"{1}\", ", "{", viewName));
            schemaBuilder.Append("\"columns\": [");

            string json = String.Concat(from r in sqlConnection.GetSchema("Columns").AsEnumerable()
                                        where r.Field<string>("TABLE_NAME") == viewName
                                        orderby r.Field<int>("ORDINAL_POSITION")
                                        select
                                        string.Format("{0} \"name\":\"{1}\", \"dataType\": \"{2}\"{3}, ", "{", r.Field<string>("COLUMN_NAME"), ConvertSqlType(r.Field<string>("DATA_TYPE")), "}")
                           );

            schemaBuilder.Append(json);
            schemaBuilder.Remove(schemaBuilder.ToString().Length - 2, 2);
            schemaBuilder.Append("]}]}");


            return schemaBuilder.ToString();
        }

        /// <summary>
        /// Convert list of datasets into .NET type
        /// </summary>        
        /// <returns>List of user datasets</returns>
        public static T Deserialize<T>(this string obj, int recursionDepth = 100)
        {
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            serializer.RecursionLimit = recursionDepth;

            string result = obj.Split(new Char[] { '[' })[1];
            result = (result.EndsWith("}")) ? result = result.Substring(0, result.Length - 1) : result;
            result = String.Format("[{0}", result);

            return serializer.Deserialize<T>(result);
        }

        public static IEnumerable<Dictionary<string, object>> Datasets(this List<Object> datasets, string name)
        {
            IEnumerable<Dictionary<string, object>> q = from d in (from d in datasets select d as Dictionary<string, object>) where d["name"] as string == name select d;

            return q;
        }

        /// <summary>
        /// Map SQL data types to matching C# types
        /// Power BI allows only 5 data types so there is some relevant mapping done 
        /// when no direct mapping was available
        /// </summary>
        /// <param name="sqlType"></param>
        /// <returns></returns>
        /// TODO#2: If data type casting like below does not work for you
        /// make the change
        private static string ConvertSqlType(string sqlType)
        {
            string jsonType = string.Empty;

            switch (sqlType)
            {
                case "int":
                case "smallint":
                case "bigint":
                    jsonType = "Int64";
                    break;
                case "decimal":
                case "float":
                case "money":
                    jsonType = "Double";
                    break;
                case "bit":
                    jsonType = "bool";
                    break;
                case "date":
                case "datetime":
                    jsonType = "DateTime";
                    break;
                case "varchar":
                case "nvarchar":
                    jsonType = "string";
                    break;
                default:
                    jsonType = "string";
                    break;
            }

            return jsonType;

        }

        /// <summary>
        /// When reading data using .net objects like data reader
        /// Map the relevant datatypes to the available Power BI data types
        /// </summary>
        /// <param name="CSType"></param>
        /// <returns></returns>
        /// TODO#3: If data type casting like below does not work for you
        /// make the change
        public static string ConvertCSType(string CSType)
        {
            string jsonType = string.Empty;
            switch (CSType)
            {
                case "Int32":
                case "Int64":
                    jsonType = "Int64";
                    break;
                case "Double":
                    jsonType = "Double";
                    break;
                case "Boolean":
                    jsonType = "bool";
                    break;
                case "DateTime":
                    jsonType = "DateTime";
                    break;
                case "String":
                    jsonType = "string";
                    break;
                default:
                    jsonType = "string";
                    break;
            }

            return jsonType;
        }
    }
}
