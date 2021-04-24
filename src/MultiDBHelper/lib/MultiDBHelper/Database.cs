#if NETFRAMEWORK
using System.Data.SqlClient;
#else
using Microsoft.Data.SqlClient;
#endif

using Dapper;
using FirebirdSql.Data.FirebirdClient;
using MultiDBHelper.Enums;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;

namespace MultiDBHelper
{
    [Obsolete("It's highly recommended to use DbSession class instead this.")]
    public static class Database
    {
        public static IEnumerable<dynamic> Query(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query(sqlQuery, paramsObj, transaction, buffered, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch 
            { 
                transaction?.Rollback(); 
                throw; 
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<object> Query(RDBMSProvider rdbmsProvider, string connectionString, Type type, string sqlQuery, object paramsObj = null, bool useTransaction = false, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query(type, sqlQuery, paramsObj, transaction, buffered, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<T> Query<T>(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.Query<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static IEnumerable<T> Query<T>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, bool buffered = true, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query<T>(sqlQuery, paramsObj, transaction, buffered, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<T> Query<T>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, Type[] types, Func<object[], T> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query(sqlQuery, types, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, Func<TFirst, TSecond, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query<TFirst, TSecond, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret = connection.Query<TFirst, TSecond, TThird, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TReturn>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query<TFirst, TSecond, TThird, TFouth, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TReturn>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, Func<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn> map, object paramsObj = null, bool useTransaction = false, bool buffered = true, string splitOn = "Id", int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Query<TFirst, TSecond, TThird, TFouth, TFifth, TSixth, TSeventh, TReturn>(sqlQuery, map, paramsObj, transaction, buffered, splitOn, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static dynamic QueryFirst(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirst(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static object QueryFirst(RDBMSProvider rdbmsProvider, Type type, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirst(type, sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T QueryFirst<T>(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.QueryFirst<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T QueryFirst<T>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirst<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static dynamic QueryFirstOrDefault(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirstOrDefault(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static object QueryFirstOrDefault(RDBMSProvider rdbmsProvider, Type type, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirstOrDefault(type, sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T QueryFirstOrDefault<T>(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.QueryFirstOrDefault<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T QueryFirstOrDefault<T>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryFirstOrDefault<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static dynamic QuerySingle(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingle(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static object QuerySingle(RDBMSProvider rdbmsProvider, Type type, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingle(type, sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T QuerySingle<T>(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.QuerySingle<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T QuerySingle<T>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingle<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static dynamic QuerySingleOrDefault(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingleOrDefault(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static object QuerySingleOrDefault(RDBMSProvider rdbmsProvider, Type type, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingleOrDefault(type, sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T QuerySingleOrDefault<T>(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.QuerySingleOrDefault<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T QuerySingleOrDefault<T>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QuerySingleOrDefault<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static SqlMapper.GridReader QueryMultiple(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.QueryMultiple(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static SqlMapper.GridReader QueryMultiple(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.QueryMultiple(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static int Execute(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.Execute(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static int Execute(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.Execute(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        
        public static object ExecuteScalar(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.ExecuteScalar(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static object ExecuteScalar(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.ExecuteScalar(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        public static T ExecuteScalar<T>(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.ExecuteScalar<T>(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static T ExecuteScalar<T>(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.ExecuteScalar<T>(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }


        public static IDataReader ExecuteReader(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.ExecuteReader(command);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static IDataReader ExecuteReader(RDBMSProvider rdbmsProvider, string connectionString, CommandDefinition command, CommandBehavior commandBehavior)
        {
            IDbConnection connection = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                return connection.ExecuteReader(command, commandBehavior);
            }
            catch { throw; }
            finally
            {
                connection.Dispose();
            }
        }

        public static IDataReader ExecuteReader(RDBMSProvider rdbmsProvider, string connectionString, string sqlQuery, object paramsObj = null, bool useTransaction = false, int? commandTimeOut = null, CommandType? commandType = null)
        {
            IDbConnection connection = null;
            IDbTransaction transaction = null;

            try
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    throw new ArgumentException("Connection string cannot be null, empty or white-space.", nameof(connectionString));

                connection = CreateConnection(rdbmsProvider, connectionString);
                connection.Open();

                if (useTransaction)
                    transaction = connection.BeginTransaction();

                var ret =  connection.ExecuteReader(sqlQuery, paramsObj, transaction, commandTimeOut, commandType);

                transaction?.Commit();

                return ret;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                connection.Dispose();
                transaction?.Dispose();
            }
        }

        
        private static IDbConnection CreateConnection(RDBMSProvider rdbmsProvider, string connectionString)
        {
            IDbConnection connection = null;

            switch (rdbmsProvider)
            {
                case RDBMSProvider.MSSQLServer:
                    connection = new SqlConnection(connectionString);
                    break;
                case RDBMSProvider.MySQL:
                    connection = new MySqlConnection(connectionString);
                    break;
                case RDBMSProvider.PostgreSQL:
                    connection = new NpgsqlConnection(connectionString);
                    break;
                case RDBMSProvider.Oracle:
                    connection = new OracleConnection(connectionString);
                    break;
                case RDBMSProvider.Firebird:
                    connection = new FbConnection(connectionString);
                    break;
                case RDBMSProvider.SQLite:
                    connection = new SQLiteConnection(connectionString);
                    break;
                default:
                    break;
            }

            return connection;
        }
    }
}
