#if NETFRAMEWORK
using System.Data.SqlClient;
#else
using Microsoft.Data.SqlClient;
#endif

using FirebirdSql.Data.FirebirdClient;
using MultiDBHelper.Enums;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.SQLite;

namespace MultiDBHelper
{
    public sealed class DbSession : IDisposable
    {
        public IDbConnection Connection { get; }
        public IDbTransaction Transaction { get; set; }

        public DbSession(RDBMSProvider rdbmsProvider, string connectionString)
        {
            Connection = CreateConnection(rdbmsProvider, connectionString);
            Connection.Open();
        }

        /// <summary>
        /// "Closes" but keeps the Connection object active, used when you need to re-open the connection soon or in a near future.
        /// </summary>
        public void Close() => Connection?.Close();

        /// <summary>
        /// Effectively closes the connection.
        /// </summary>
        public void Dispose() => Connection?.Dispose();

        private IDbConnection CreateConnection(RDBMSProvider rdbmsProvider, string connectionString)
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
