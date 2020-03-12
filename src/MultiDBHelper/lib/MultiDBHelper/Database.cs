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

        public static IEnumerable<dynamic> Query(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, 
            int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query(sqlQuery, paramsObj, sqlTransaction, buffered, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<object> Query(Db db, string connectionString, Type type, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, 
            bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query(type, sqlQuery, paramsObj, sqlTransaction, buffered, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<T> Query<T>(Db db, string connectionString, CommandDefinition command)
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
                return connection.Query<T>(command);
            }
        }

        public static IEnumerable<T> Query<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, 
            int? commandTimeOut = null, CommandType? commandType = null)
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

        public static IEnumerable<T> Query<T>(Db db, string connectionString, string sqlQuery, Type[] types, Func<object[], T> map, object paramsObj = null, 
            IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query<T>(sqlQuery, types, map, paramsObj, sqlTransaction, buffered, splitOn, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TReturn> map, object paramsObj = null,
            IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query<TFirst, TSecond, TReturn>(sqlQuery, map, paramsObj, sqlTransaction, buffered, splitOn, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TReturn> map, object paramsObj = null,
            IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query<TFirst, TSecond, TThird, TReturn>(sqlQuery, map, paramsObj, sqlTransaction, buffered, splitOn, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TReturn> map, object paramsObj = null,
            IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query<TFirst, TSecond, TThird, TFouth, TReturn>(sqlQuery, map, paramsObj, sqlTransaction, buffered, splitOn, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TReturn> map, object paramsObj = null,
            IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TReturn>(sqlQuery, map, paramsObj, sqlTransaction, buffered, splitOn, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn> map, object paramsObj = null,
            IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn>(sqlQuery, map, paramsObj, sqlTransaction, buffered, splitOn, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn> map, object paramsObj = null,
            IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn>(sqlQuery, map, paramsObj, sqlTransaction, buffered, splitOn, commandTimeOut, commandType);
            }
        }

        
        public static T QueryFirstOrDefault<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QueryFirstOrDefault(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static dynamic QueryFirstOrDefault(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QueryFirstOrDefault(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static object QueryFirstOrDefault(Db db, Type type, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QueryFirstOrDefault(type, sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
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
