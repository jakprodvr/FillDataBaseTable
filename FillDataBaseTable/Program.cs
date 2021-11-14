using System;
using System.Text;
using System.Data.SqlClient;
using System.Data;


namespace FillDataBaseTable
{
    public delegate QQuery QueryMakerSqlParameter(string line);

    /// <summary>
    /// This class encapsulates two public objects,SqlParameter[] sqlparameters and string query
    /// </summary>
    public class QQuery
    {
        public SqlParameter[] sqlparameters;
        public string query;
        public QQuery(string query, SqlParameter[] sqlparameters)
        {
            this.query = query;
            this.sqlparameters = sqlparameters;
        }
        public QQuery()
        {
            this.query = null;
            this.sqlparameters = null;
        }
    }
    /// <summary>
    /// This class has the methods to insert records in the database [Job01Structure]
    /// </summary>
    public class Fill_Job01StructureDB 
    {
        /// <summary>
        /// this method creates a query to insert records in the table [Department]
        /// </summary>
        /// <param name="line">It is the result of reading a line in the file where it is located the data that will be inserted in the table:
        /// dep_buildinglevel  int, dep_name varchar(20),dep_manager  varchar(30)</param>
        /// <returns></returns>
        public QQuery MakeInsertion_Depart_Tb(string line)
        {
            string query = "INSERT INTO Department(dep_buildinglevel,dep_name,dep_manager) " +
                "VALUES(@dep_buildinglevel,@dep_name,@dep_manager)";

            string[] string_values = line.Split(',');

            SqlParameter[] sqlparameters = new SqlParameter[3];

            sqlparameters[0] = new SqlParameter("@dep_buildinglevel", SqlDbType.Int);
            sqlparameters[0].Value = int.Parse(string_values[0]);

            sqlparameters[1] = new SqlParameter("@dep_name", SqlDbType.VarChar, 20);
            sqlparameters[1].Value = string_values[1];

            sqlparameters[2] = new SqlParameter("@dep_manager", SqlDbType.VarChar, 20);
            sqlparameters[2].Value = string_values[2];

            return new QQuery(query, sqlparameters);
        }

        /// <summary>
        /// this method creates a query to insert records in the table [Employees]
        /// </summary>
        /// <param name="line">It is the result of reading a line in the file where it is located the data that will be inserted in the table:
        /// dep_id int,emp_name varchar(30),emp_hiredate  date(yyyy/mm/dd),emp_salary  decimal(10,2)</param>
        /// <returns></returns>
        public QQuery MakeInsertion_Employ_Tb(string line)
        {
            string query = "INSERT INTO Employees(dep_id,emp_name,emp_hiredate,emp_salary) " +
                            "VALUES(@dep_id,@emp_name,@emp_hiredate,@emp_salary)";

            string[] string_values = line.Split(',');

            SqlParameter[] sqlparameters = new SqlParameter[4];

            sqlparameters[0] = new SqlParameter("@dep_id", SqlDbType.Int);
            sqlparameters[0].Value = int.Parse(string_values[0]);


            sqlparameters[1] = new SqlParameter("@emp_name", SqlDbType.VarChar, 30);
            sqlparameters[1].Value = string_values[1];

            sqlparameters[2] = new SqlParameter("@emp_hiredate", SqlDbType.Date);
            sqlparameters[2].Value = string_values[2];

            sqlparameters[3] = new SqlParameter("@emp_salary", SqlDbType.Decimal);
            sqlparameters[3].Value = Decimal.Parse(string_values[3]);
            sqlparameters[3].Precision = 8;
            sqlparameters[3].Scale = 4;

            return new QQuery(query, sqlparameters);
        }
    }

    /// <summary>
    /// This class creates the connection to the database in which the data will be inserted
    /// </summary>
    public class FillTable
    {
        public SqlConnectionStringBuilder Connection_string;
        public StringBuilder sb_query;
        public string CSV_file_path;       
        int rowsAffected;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="builder"> ConnectionString for the database </param>
        /// <param name="csv_file_path">path of the file from which the data will be read. CSV file</param>
        public FillTable(SqlConnectionStringBuilder builder, string csv_file_path)
        {
            this.Connection_string = builder;
            this.CSV_file_path = csv_file_path;
            sb_query = new StringBuilder();
            rowsAffected = 0;
        }
               
        public string CsvFilePath
        {
            get { return CSV_file_path; }
            set { CSV_file_path = value; }
        }

        /// <summary>
        /// This method creates a StreamReader to read the data from a csv file and creates the connection to the target database
        /// </summary>
        /// <param name="queryparameter">Reference to a method that will query the specific table </param>
        /// <returns></returns>
        public int InsertIntoTable(QueryMakerSqlParameter queryparameter)
        {
            using (SqlConnection connection = new SqlConnection(Connection_string.ConnectionString))
            {
                try
                {
                    connection.Open();
                    Console.WriteLine(" connection Open Done.");

                    using (System.IO.StreamReader sr = new System.IO.StreamReader(CSV_file_path))
                    {
                        string currentLine = sr.ReadLine();
                        currentLine = sr.ReadLine();
                        QQuery qquery = new QQuery();

                        while (currentLine != null)
                        {
                            //Console.WriteLine(currentLine);

                            qquery = queryparameter(currentLine);
                            SqlParameter[] arr_par = qquery.sqlparameters;

                            using (SqlCommand command = new SqlCommand(qquery.query, connection))
                            {
                                foreach (SqlParameter sqlp in arr_par)
                                {
                                    command.Parameters.Add(sqlp);
                                }
                                command.Prepare();
                                rowsAffected += command.ExecuteNonQuery();
                                 Console.WriteLine(rowsAffected + " row(s) inserted");
                                // Console.WriteLine("Done.");
                            }
                            currentLine = sr.ReadLine();
                        }
                    }
                    connection.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            return rowsAffected;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            string Department_file_path = "D:/Programacion/Estudiando_Ahora/Data_base/SQL/microsoft_sql_server/randomdata/Job01Structure/Job01Structure_Department05.csv";
            string Employee_file_path = "D:/Programacion/Estudiando_Ahora/Data_base/SQL/microsoft_sql_server/randomdata/Job01Structure/Job01Structure_Employees05.csv";

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = "";        //update
            builder.UserID = "";            //update
            builder.Password = "";          //update
            builder.InitialCatalog = "";    //update

            FillTable ft = new FillTable(builder,Employee_file_path);
            Fill_Job01StructureDB job01est_db = new Fill_Job01StructureDB();              
            int rowafeec = ft.InsertIntoTable(job01est_db.MakeInsertion_Employ_Tb);

            Console.WriteLine("Hello World!");
        }
    }
}
