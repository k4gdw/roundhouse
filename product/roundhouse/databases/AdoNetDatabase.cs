// AdoNetDatabase.cs
// ***********************************************************************
// Assembly: roundhouse
// Author  : Bryan Johns
// Created : 03-08-2016
// ***********************************************************************
// Last Modified By: Bryan Johns
// Last Modified On 03-09-2016
// ***********************************************************************
// <copyright file="AdoNetDatabase.cs" 
//            company="Alabama Department of Mental Health">
//      Copyright © 2016, ADMH. All rights reserved.
// </copyright>
// <summary></summary>
// *************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using roundhouse.connections;
using roundhouse.infrastructure.app;
using roundhouse.infrastructure.logging;
using roundhouse.parameters;

namespace roundhouse.databases{
    public abstract class AdoNetDatabase : DefaultDatabase<IDbConnection>{
        private const int SqlConnectionExceptionNumber = 233;

        public override bool split_batch_statements { get; set; } = true;

        protected IDbTransaction Transaction;

        private DbProviderFactory _providerFactory;

        private AdoNetConnection GetAdoNetConnection(string connString){
            _providerFactory = DbProviderFactories.GetFactory(provider);
            IDbConnection connection = _providerFactory.CreateConnection();
            connection_specific_setup(connection);

            if (connection == null) return null;
            connection.ConnectionString = connString;
            return new AdoNetConnection(connection);
        }

        protected virtual void connection_specific_setup(IDbConnection connection) {}

        public override void open_admin_connection(){
            Log.bound_to(this)
                .log_a_debug_event_containing("Opening admin connection to '{0}'", admin_connection_string);
            AdminConnection = GetAdoNetConnection(admin_connection_string);
            AdminConnection.open();
        }

        public override void close_admin_connection(){
            Log.bound_to(this).log_a_debug_event_containing("Closing admin connection");
            if (AdminConnection != null){
                AdminConnection.clear_pool();
                AdminConnection.close();
                AdminConnection.Dispose();
                AdminConnection = null;
            }
        }

        public override void open_connection(bool withTransaction){
            Log.bound_to(this).log_a_debug_event_containing("Opening connection to '{0}'", connection_string);
            ServerConnection = GetAdoNetConnection(connection_string);
            ServerConnection.open();
            if (withTransaction){
                Transaction = ServerConnection.underlying_type().BeginTransaction();
            }

            set_repository();
            Repository?.start(withTransaction);
        }

        public override void close_connection(){
            Log.bound_to(this).log_a_debug_event_containing("Closing connection");
            if (Transaction != null){
                Transaction.Commit();
                Transaction = null;
            }
            Repository?.finish();

            if (ServerConnection == null) return;
            ServerConnection.clear_pool();
            ServerConnection.close();
            ServerConnection.Dispose();
            ServerConnection = null;
        }

        public override void rollback(){
            Log.bound_to(this).log_a_debug_event_containing("Rolling back changes");
            Repository.rollback();

            if (Transaction != null){
                //rollback previous transaction
                Transaction.Rollback();
                ServerConnection.close();

                //open a new transaction
                ServerConnection.open();
                //use_database(database_name);
                Transaction = ServerConnection.underlying_type().BeginTransaction();
                Repository.start(true);
            }
        }

        protected override void run_sql(string sqlToRun,
            ConnectionType connectionType, IList<IParameter<IDbDataParameter>> parameters){
            if (string.IsNullOrEmpty(sqlToRun)) return;

            //really naive retry logic. Consider Lokad retry policy
            //this is due to sql server holding onto a connection http://social.msdn.microsoft.com/Forums/en-US/adodotnetdataproviders/thread/99963999-a59b-4614-a1b9-869c6dff921e
            try{
                run_command_with(sqlToRun, connectionType, parameters);
            }
            catch (SqlException ex){
                // If we are not running inside a transaction, then we can continue to the next command.
                if (Transaction == null){
                    // But only if it's a connection failure AND connection failure is the only error reported.
                    if (ex.Errors.Count == 1 && ex.Number == SqlConnectionExceptionNumber){
                        Log.bound_to(this)
                            .log_a_debug_event_containing("Failure executing command, trying again. {0}{1}",
                                Environment.NewLine, ex.ToString());
                        run_command_with(sqlToRun, connectionType, parameters);
                    }
                    else{
                        //Re-throw the original exception.
                        throw;
                    }
                }
                else{
                    // Re-throw the exception, which will delegate handling of the rollback to DatabaseMigrator calling class,
                    // e.g. DefaultDatabaseMigrator.run_sql(...) method catches exceptions from run_sql and rolls back the transaction.
                    throw;
                }
            }
        }

        private void run_command_with(string sqlToRun,
            ConnectionType connectionType,
            IList<IParameter<IDbDataParameter>> parameters){
            using (var command = SetupDatabaseCommand(sqlToRun, connectionType, parameters)){
                command.ExecuteNonQuery();
                command.Dispose();
            }
        }

        protected override object run_sql_scalar(string sqlToRun,
            ConnectionType connectionType,
            IList<IParameter<IDbDataParameter>> parameters){
            var returnValue = new object();
            if (string.IsNullOrEmpty(sqlToRun)) return returnValue;

            using (var command = SetupDatabaseCommand(sqlToRun, connectionType, null)){
                returnValue = command.ExecuteScalar();
                command.Dispose();
            }

            return returnValue;
        }

        protected IDbCommand SetupDatabaseCommand(string sqlToRun,
                                                  ConnectionType connectionType,
                                                  IEnumerable<IParameter<IDbDataParameter>> parameters){
            IDbCommand command = null;
            switch (connectionType){
                case ConnectionType.Default:
                    if (ServerConnection == null || ServerConnection.underlying_type().State != ConnectionState.Open){
                        open_connection(false);
                    }
                    Log.bound_to(this).log_a_debug_event_containing("Setting up command for normal connection");
                    command = ServerConnection.underlying_type().CreateCommand();
                    command.CommandTimeout = command_timeout;
                    break;
                case ConnectionType.Admin:
                    if (AdminConnection == null || AdminConnection.underlying_type().State != ConnectionState.Open){
                        open_admin_connection();
                    }
                    Log.bound_to(this).log_a_debug_event_containing("Setting up command for admin connection");
                    command = AdminConnection.underlying_type().CreateCommand();
                    command.CommandTimeout = admin_command_timeout;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(connectionType), connectionType, null);
            }

            if (parameters != null){
                foreach (var parameter in parameters){
                    command.Parameters.Add(parameter.underlying_type);
                }
            }
            command.Transaction = Transaction;
            command.CommandText = sqlToRun;
            command.CommandType = CommandType.Text;

            return command;
        }
    }
}