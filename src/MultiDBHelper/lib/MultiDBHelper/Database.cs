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
        public static IEnumerable<dynamic> Query(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query(sqlQuery, paramsObj, transaction, buffered, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<object> Query(Db db, string connectionString, Type type, string sqlQuery, object paramsObj = null, bool useTransaction = false, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query(type, sqlQuery, paramsObj, transaction, buffered, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<T> Query<T>(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.Query<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static IEnumerable<T> Query<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query<T>(sqlQuery, paramsObj, transaction, buffered, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<T> Query<T>(Db db, string connectionString, string sqlQuery, Type[] types, Func<object[], T> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query(sqlQuery, types, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query<TFirst, TSecond, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query<TFirst, TSecond, TThird, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query<TFirst, TSecond, TThird, TFouth, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn>(Db db, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static dynamic QueryFirst(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirst(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static object QueryFirst(Db db, Type type, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirst(type, sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T QueryFirst<T>(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.QueryFirst<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T QueryFirst<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirst<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static dynamic QueryFirstOrDefault(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirstOrDefault(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static object QueryFirstOrDefault(Db db, Type type, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirstOrDefault(type, sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T QueryFirstOrDefault<T>(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.QueryFirstOrDefault<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T QueryFirstOrDefault<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirstOrDefault<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static dynamic QuerySingle(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingle(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static object QuerySingle(Db db, Type type, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingle(type, sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T QuerySingle<T>(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.QuerySingle<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T QuerySingle<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingle<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static dynamic QuerySingleOrDefault(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingleOrDefault(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static object QuerySingleOrDefault(Db db, Type type, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingleOrDefault(type, sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T QuerySingleOrDefault<T>(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.QuerySingleOrDefault<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T QuerySingleOrDefault<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingleOrDefault<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static SqlMapper.GridReader QueryMultiple(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.QueryMultiple(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static SqlMapper.GridReader QueryMultiple(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryMultiple(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static int Execute(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Execute(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static int Execute(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.Execute(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        
        public static object ExecuteScalar(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.ExecuteScalar(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static object ExecuteScalar(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.ExecuteScalar(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T ExecuteScalar<T>(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.ExecuteScalar<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T ExecuteScalar<T>(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.ExecuteScalar<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static IDataReader ExecuteReader(Db db, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.ExecuteReader(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static IDataReader ExecuteReader(Db db, string connectionString, CommandDefinition command, CommandBehavior commandBehavior)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                return connection.ExecuteReader(command, commandBehavior);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static IDataReader ExecuteReader(Db db, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

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

                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.ExecuteReader(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }
    }
}
