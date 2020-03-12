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
                return connection.Query(sqlQuery, paramsObj, sqlTransaction, buffered, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<object> Query(Db db, string connectionString, Type type, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
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

        public static IEnumerable<T> Query<T>(Db db, string connectionString, string sqlQuery, Type[] types, Func<object[], T> map, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.Query(sqlQuery, types, map, paramsObj, sqlTransaction, buffered, splitOn, commandTimeOut, commandType);
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TReturn> map, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TReturn> map, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TReturn> map, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TReturn> map, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn> map, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn> map, object paramsObj = null, IDbTransaction sqlTransaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
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


        public static dynamic QueryFirst(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QueryFirst(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static object QueryFirst(Db db, Type type, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QueryFirst(type, sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static T QueryFirst<T>(Db db, string connectionString, CommandDefinition command)
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
                return connection.QueryFirst<T>(command);
            }
        }

        public static T QueryFirst<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QueryFirst<T>(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
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

        public static T QueryFirstOrDefault<T>(Db db, string connectionString, CommandDefinition command)
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
                return connection.QueryFirstOrDefault<T>(command);
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
                return connection.QueryFirstOrDefault<T>(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }


        public static dynamic QuerySingle(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QuerySingle(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static object QuerySingle(Db db, Type type, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QuerySingle(type, sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static T QuerySingle<T>(Db db, string connectionString, CommandDefinition command)
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
                return connection.QuerySingle<T>(command);
            }
        }

        public static T QuerySingle<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QuerySingle<T>(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }


        public static dynamic QuerySingleOrDefault(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QuerySingleOrDefault(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static object QuerySingleOrDefault(Db db, Type type, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QuerySingleOrDefault(type, sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static T QuerySingleOrDefault<T>(Db db, string connectionString, CommandDefinition command)
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
                return connection.QuerySingleOrDefault<T>(command);
            }
        }

        public static T QuerySingleOrDefault<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QuerySingleOrDefault<T>(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }


        public static SqlMapper.GridReader QueryMultiple(Db db, string connectionString, CommandDefinition command)
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
                return connection.QueryMultiple(command);
            }
        }

        public static SqlMapper.GridReader QueryMultiple(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.QueryMultiple(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
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

        public static int Execute(Db db, string connectionString, CommandDefinition command)
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
                return connection.Execute(command);
            }
        }

        
        public static object ExecuteScalar(Db db, string connectionString, CommandDefinition command)
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
                return connection.ExecuteScalar(command);
            }
        }

        public static object ExecuteScalar(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.ExecuteScalar(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }

        public static T ExecuteScalar<T>(Db db, string connectionString, CommandDefinition command)
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
                return connection.ExecuteScalar<T>(command);
            }
        }

        public static T ExecuteScalar<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.ExecuteScalar<T>(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }


        public static IDataReader ExecuteReader(Db db, string connectionString, CommandDefinition command)
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
                return connection.ExecuteReader(command);
            }
        }

        public static IDataReader ExecuteReader(Db db, string connectionString, CommandDefinition command, CommandBehavior commandBehavior)
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
                return connection.ExecuteReader(command, commandBehavior);
            }
        }

        public static IDataReader ExecuteReader(Db db, string connectionString, string sqlQuery, object paramsObj = null, IDbTransaction sqlTransaction = null, int? commandTimeOut = null, CommandType? commandType = null)
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
                return connection.ExecuteReader(sqlQuery, paramsObj, sqlTransaction, commandTimeOut, commandType);
            }
        }
    }
}
