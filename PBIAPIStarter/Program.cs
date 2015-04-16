using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

using System.Threading.Tasks;
using PBIAPIStarter.Util;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System.Data.SqlClient;
using System.Web.Script.Serialization;

namespace PBIAPIStarter
{
    class Program
    {
        // See How to register an app (http://go.microsoft.com/fwlink/?LinkId=519361)
        // There are few changes required to make this tool work for you. 
        // Search for TODO string the code and make changes per your requirement


         //TODO#1: Replace clientID with your client app ID. To learn how to get a client app ID, see How to register an app (http://go.microsoft.com/fwlink/?LinkId=519361)
        private static string clientID = "";


        //Replace redirectUri with the uri you used when you registered your app https://login.live.com/oauth20_desktop.srf
        private static string redirectUri = "https://login.live.com/oauth20_desktop.srf";

        //Power BI resource uri
        private static string resourceUri = "https://analysis.windows.net/powerbi/api";
        //OAuth2 authority
        private static string authority = "https://login.windows.net/common/oauth2/authorize";
        //Uri for Power BI datasets
        private static string datasetsUri = "https://api.powerbi.com/beta/myorg/datasets";

        private static AuthenticationContext authContext = null;
        private static string token = String.Empty;

        // Dataset Name
        private static string datasetName = ""; 
        
        //SQL DB Connection information:        
        private static string sqlserverName = "";
        private static string databaseName = "";       
        private static string tableName = ""; 
       
        static void Main(string[] args)
        {
            // Test the connection and update the datasetsUri in case of redirect
            // This allows user authentication and consent if consent is required
            datasetsUri = TestConnection();

            List<Object> datasets = GetAllDatasets();

            Console.WriteLine("Welcome.....");
            Console.WriteLine("Here are the existing datasets on your PowerBI Site:");

            foreach (Dictionary<string, object> obj in datasets)
            {
                Console.WriteLine(String.Format("Name: {0}", obj["name"]));
            }

            if (args.Length == 0)
            {
                Console.WriteLine("Provide existing dataset name to refresh or provide a new dataset name to create one.");
                datasetName = Console.ReadLine();
                //Initiate pushing of rows to Power BI
                Console.WriteLine("Provide SQL Data Source connection information....");
                Console.WriteLine("SQL server name:");
                sqlserverName = Console.ReadLine();

                Console.WriteLine("Database name:");
                databaseName = Console.ReadLine();

                Console.WriteLine("Table/View name (format SCHEMA.TABLENAME) (Note*: This is case-sensitive):");
                tableName = Console.ReadLine();
            }

            try
            {
                CreateFromSqlSchema();

                //Clear rows if the dataset already exists. 
                ClearRows();                         
                
                AddSQLRowsMultiPush();

            }
            catch (Exception ex)
            {
                // Database connection errors, bad request error due to data serialization are possible errors
                Console.WriteLine("OOPS! Something went wrong, Please try again. Below is more information for your reference.");
                Console.WriteLine(ex.Message);
            }
            // Finished pushing rows to Power BI, close the console window
            Console.WriteLine("Press the Enter key to close this window.");
            Console.ReadLine();

        }

        /// <summary>
        /// Create a Power BI schema from a SQL View.        
        /// </summary>
        static void CreateFromSqlSchema()
        {
            SqlConnectionStringBuilder connStringBuilder = new SqlConnectionStringBuilder();
            connStringBuilder.IntegratedSecurity = true;
            connStringBuilder.DataSource = sqlserverName;
            connStringBuilder.InitialCatalog = databaseName;

            using (SqlConnection conn = new SqlConnection(connStringBuilder.ConnectionString))
            {
                conn.Open();

                //Better to add more specific exception handling.           
                try
                {
                    //Create a POST web request to list all datasets
                    HttpWebRequest request = DatasetRequest(datasetsUri, "POST", AccessToken);

                    var datasets = GetAllDatasets().Datasets(datasetName);

                    Console.WriteLine("Checking Data Connection.....");

                    string json = conn.ToJson(datasetName, tableName);

                    // If data connection is successful and column details are read 
                    if (json.Length != 0 && json.Contains("dataType"))
                        Console.WriteLine("Data connection is successful.");
                    // Check if the dataset exists
                    if (datasets.Count() == 0)
                    {
                        //POST request using the json schema from Product
                        string response=PostRequest(request, json);
                        Console.WriteLine("New Dataset created.");
                    }
                    else
                    {
                        Console.WriteLine("Updating the {0} dataset." , datasetName);
                    }
                }
                catch (Exception ex)
                {   
                    throw ex;
                }
            }
        }

        private static string TestConnection()
        {
            // Check the connection for redirects
            HttpWebRequest request = System.Net.WebRequest.Create(datasetsUri) as System.Net.HttpWebRequest;
            request.KeepAlive = true;
            request.Method = "GET";
            request.ContentLength = 0;
            request.ContentType = "application/json";
            request.Headers.Add("Authorization", String.Format("Bearer {0}", AccessToken));
            request.AllowAutoRedirect = false;

            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            if (response.StatusCode == HttpStatusCode.TemporaryRedirect)
            {
                return response.Headers["Location"];
            }
            return datasetsUri;

        }

        /// <summary>
        /// Pushes data in chunks of 5000 rows or data of size about 2097152
        /// Better to separate database access code from here
        /// </summary>
        static void AddSQLRowsMultiPush()
        {            
            SqlConnectionStringBuilder connStringBuilder = new SqlConnectionStringBuilder();
            connStringBuilder.IntegratedSecurity = true;

            //ConnectionBuilderActivity
            connStringBuilder.DataSource = sqlserverName;
            connStringBuilder.InitialCatalog = databaseName;

            //Get dataset id from dataset name
            string datasetId = GetAllDatasets().Datasets(datasetName).First()["id"].ToString();

            Console.WriteLine("Posting data to dataset...");
            using (SqlConnection connection = new SqlConnection(connStringBuilder.ToString()))
            {
                //Next Iteration: SQL DB - Reliable Connection
                using (SqlCommand command = connection.CreateCommand())
                {
                    connection.Open();

                    command.CommandText = String.Format("SELECT {0} FROM {1}", "*", tableName);

                    //Next Iteration: Show ExecuteReaderAsync
                    SqlDataReader reader = command.ExecuteReader();

                    StringBuilder jsonBuilder = new StringBuilder();                    

                    var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();

                    int length = columnNames.Count;

                    StringBuilder res = new StringBuilder();
                    int currentrow = 0;
                    int currentCharacterSize = 0;

                    List<Dictionary<string, object>> currentrowset = new List<Dictionary<string, object>>();

                    // Read row by row and when the request limit is reached push data until that rows
                    // and continue reading the remaining data
                    while (reader.Read())
                    {
                        // Add Json string to identify rows section
                        if (jsonBuilder.Length == 0)
                        {
                            jsonBuilder.Append(string.Format("{0}\"rows\":", "{"));
                        }

                        Dictionary<string, object> currentRowData = new Dictionary<string, object>();

                        for (int i = 0; i < length; i++)
                        {
                            string[] NumberTypes = { "Int64", "Double" };
                            // Power BI API throws error if blank string is send for numeric values so handle the numeric
                            // data types and send "0"
                            object cellValue = NumberTypes.Contains(PBIHelper.ConvertCSType(reader.GetFieldType(i).Name)) && reader[columnNames[i]].ToString().Equals(string.Empty) ? "0" : reader[columnNames[i]];                            
                            currentRowData.Add(columnNames[i], cellValue);
                            // quotes, colon and comma for each cell  additional 5 or 6 characters
                            currentCharacterSize += (columnNames[i].Length + cellValue.ToString().Length + 6);
                        }
                        
                        currentrowset.Add(currentRowData);
                        currentrow++;
                        // POWER BI REST API support 10000 Max rows per single push
                        // But in case of table with a lot of columns esp like text..etc the character limit may cross for less than 10000 rows. 
                        // Hence 5000 row limit is decided to send more frequent pushes and this make user experience a frequent refresh of dashboards
                        // This limit can be changed per the requirement since 2097152 size is also verified for determining chunk to push
                        if (currentrow == 5000 || !reader.HasRows || currentCharacterSize >= 2097152)
                        {
                            
                            JavaScriptSerializer serializer = new JavaScriptSerializer();
                            // If current data size is slightly higher than default limit of 2097152
                            // Better to push data in chunks of the size around default limit 
                            serializer.MaxJsonLength = Int32.MaxValue;
                            string values = serializer.Serialize(currentrowset);
                            // Add rows data to the jsonstring
                            jsonBuilder.Append(values);

                            // Close the rows section
                            jsonBuilder.Append(string.Format("{0}", "}"));

                            //Push data in small chunks in case of big size rows or more than 5k rows in the given table 
                            try
                            {
                                HttpWebRequest request = DatasetRequest(String.Format("{0}/{1}/tables/{2}/rows", datasetsUri, datasetId, tableName.Substring(tableName.IndexOf(".") + 1)), "POST", AccessToken);

                                //POST request using the json 
                                Console.WriteLine(PostRequest(request, jsonBuilder.ToString()));

                                if ((currentrow < 5000 && currentCharacterSize >= 2097152) || (currentrow == 5000))
                                    Console.WriteLine("Added {0} Rows to the dataset {1} and more rows are being added.", currentrow, datasetName);

                                currentrowset.Clear();
                                jsonBuilder.Clear();
                                currentCharacterSize = 0;
                                currentrow = 0;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("OOPS! Something went wrong, Please try again later. Below is more information for your reference.");
                                Console.WriteLine(ex.Message);
                                break;
                            }
                        }
                    }

                    // Push all the records once if total rows < 5000
                    if (!reader.Read())
                    {
                        //post only if there are rows
                        if( currentrowset.Count > 0)
                        { 
                            try
                            {
                                HttpWebRequest request = DatasetRequest(String.Format("{0}/{1}/tables/{2}/rows", datasetsUri, datasetId, tableName.Substring(tableName.IndexOf(".") + 1)), "POST", AccessToken);


                                JavaScriptSerializer serializer = new JavaScriptSerializer();
                                // If current data size is higher than default limit of 2097152
                                serializer.MaxJsonLength = Int32.MaxValue;
                                string values = serializer.Serialize(currentrowset);
                                // Add rows data
                                jsonBuilder.Append(values);
                                // Close rows section
                                jsonBuilder.Append(string.Format("{0}", "}"));

                                //POST request using the json 
                                Console.WriteLine(PostRequest(request, jsonBuilder.ToString()));

                                Console.WriteLine("Added All {0} Rows to the dataset {1} and process completed.", currentrow, datasetName);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("OOPS! Something went wrong, Please try again later. Below is more information for your reference.");
                                Console.WriteLine(ex.Message);
                            }
                        }

                    }

                }
            }


        }

        // Based on MSDN article about Power BI API
        static string AccessToken
        {
            get
            {

                if (token == String.Empty)
                {
                    TokenCache TC = new TokenCache();
                    authContext = new AuthenticationContext(authority, TC);
                    token = authContext.AcquireToken(resourceUri, clientID, new Uri(redirectUri)).AccessToken.ToString();
                }
                else
                {
                    token = authContext.AcquireTokenSilent(resourceUri, clientID).AccessToken;
                }

                return token;
            }
        }

        static List<Object> GetAllDatasets()
        {
            List<Object> datasets = null;

            // use more specific exception handling.
            try
            {
                //Create a GET web request to list all datasets
                HttpWebRequest request = DatasetRequest(datasetsUri, "GET", AccessToken);

                //Get HttpWebResponse from GET request
                string responseContent = GetResponse(request);

                //Get list from response
                datasets = responseContent.Deserialize<List<Object>>();

            }
            catch (Exception ex)
            {
                // 
                Console.WriteLine("OOPS! Something went wrong, Please try again later. Below is more information for your reference.");
                Console.WriteLine(ex.Message);
            }

            return datasets;
        }


        static void ClearRows()
        {
            //Get dataset id from a table name
            string datasetId = GetAllDatasets().Datasets(datasetName).First()["id"].ToString();

            // Better to add more specific exception handling
            try
            {
                //Create a DELETE web request
                HttpWebRequest request = DatasetRequest(String.Format("{0}/{1}/tables/{2}/rows", datasetsUri, datasetId, tableName.Substring(tableName.IndexOf(".") + 1)), "DELETE", AccessToken);
                request.ContentLength = 0;

                Console.WriteLine(GetResponse(request));
            }
            catch (Exception ex)
            {
                Console.WriteLine("OOPS! Something went wrong, Please try again later. Below is more information for your reference.");
                Console.WriteLine(ex.Message);
            }
        }

        private static string PostRequest(HttpWebRequest request, string json)
        {
            byte[] byteArray = System.Text.Encoding.UTF8.GetBytes(json);
            request.ContentLength = byteArray.Length;

            //Write JSON byte[] into a Stream
            using (Stream writer = request.GetRequestStream())
            {
                writer.Write(byteArray, 0, byteArray.Length);
            }
            return GetResponse(request);
        }

        private static string GetResponse(HttpWebRequest request)
        {
            string response = string.Empty;

            using (HttpWebResponse httpResponse = request.GetResponse() as System.Net.HttpWebResponse)
            {
                //Get StreamReader that holds the response stream
                using (StreamReader reader = new System.IO.StreamReader(httpResponse.GetResponseStream()))
                {
                    response = reader.ReadToEnd();
                }
            }

            return response;
        }

        private static HttpWebRequest DatasetRequest(string datasetsUri, string method, string authorizationToken)
        {
            HttpWebRequest request = System.Net.WebRequest.Create(datasetsUri) as System.Net.HttpWebRequest;
            request.KeepAlive = true;
            request.Method = method;
            request.ContentLength = 0;
            request.ContentType = "application/json";
            request.Headers.Add("Authorization", String.Format("Bearer {0}", authorizationToken));

            return request;
        }
    }
}

