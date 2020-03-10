using Dapper;
using MultiDBHelper.Enums;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using FirebirdSql.Data.FirebirdClient;
using System;

namespace MultiDBHelper
{
    public static class Database
    {

        public static IEnumerable<T> Query<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));
            
            IDbConnection connection = null;

            switch (db)
            {
                case Db.MSSQLServer:
                    connection = new SqlConnection(connectionString);
                    break;
                case Db.MySQL:
                    connection = new MySqlConnection(connectionString);
                    break;
                case Db.PostgreSQL:
                    connection = new NpgsqlConnection(connectionString);
                    break;
                case Db.Oracle:
                    connection = new OracleConnection(connectionString);
                    break;
                case Db.Firebird:
                    connection = new FbConnection(connectionString);
                    break;
                case Db.SQLite:
                    connection = new SQLiteConnection(connectionString);
                    break;
                default:
                    break;
            }

            using (connection)
            {
                return connection.Query<T>(sqlQuery, paramsObj, sqlTransaction, buffered, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<dynamic> Query(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

            IDbConnection connection = null;

            switch (db)
            {
                case Db.MSSQLServer:
                    connection = new SqlConnection(connectionString);
                    break;
                case Db.MySQL:
                    connection = new MySqlConnection(connectionString);
                    break;
                case Db.PostgreSQL:
                    connection = new NpgsqlConnection(connectionString);
                    break;
                case Db.Oracle:
                    connection = new OracleConnection(connectionString);
                    break;
                case Db.Firebird:
                    connection = new FbConnection(connectionString);
                    break;
                case Db.SQLite:
                    connection = new SQLiteConnection(connectionString);
                    break;
                default:
                    break;
            }

            using (connection)
            {
                return connection.Query<dynamic>(sqlQuery, paramsObj, sqlTransaction, buffered, commandTimeOut, commandType);
            }
        }

        public static int Execute(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

            IDbConnection connection = null;

            switch (db)
            {
                case Db.MSSQLServer:
                    connection = new SqlConnection(connectionString);
                    break;
                case Db.MySQL:
                    connection = new MySqlConnection(connectionString);
                    break;
                case Db.PostgreSQL:
                    connection = new NpgsqlConnection(connectionString);
                    break;
                case Db.Oracle:
                    connection = new OracleConnection(connectionString);
                    break;
                case Db.Firebird:
                    connection = new FbConnection(connectionString);
                    break;
                case Db.SQLite:
                    connection = new SQLiteConnection(connectionString);
                    break;
                default:
                    break;
            }

            using (connection)
            {
                return connection.Execute(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }
    }
}
