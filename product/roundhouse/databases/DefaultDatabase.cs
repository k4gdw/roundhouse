#region File Header

// DefaultDatabase.cs
// ***********************************************************************
// Assembly: roundhouse
// Author    : Bryan Johns
// Created   : 03-08-2016
// ***********************************************************************
// Last Modified By: Bryan Johns
// Last Modified On 03-10-2016
// ***********************************************************************
// <copyright file="DefaultDatabase.cs" 
//            company="Alabama Department of Mental Health">
//      Copyright © 2016, ADMH. All rights reserved.
// </copyright>
// <summary></summary>
// *************************************************************************

#endregion

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using NHibernate.Cfg;
using NHibernate.Criterion;
using NHibernate.Tool.hbm2ddl;
using roundhouse.connections;
using roundhouse.infrastructure.app;
using roundhouse.infrastructure.app.tokens;
using roundhouse.infrastructure.extensions;
using roundhouse.infrastructure.logging;
using roundhouse.infrastructure.persistence;
using roundhouse.model;
using roundhouse.parameters;
using roundhouse.sqlsplitters;
using Environment = System.Environment;
using Version = roundhouse.model.Version;

namespace roundhouse.databases {
    public abstract class DefaultDatabase<TDbConnection> : Database {
        private bool _disposing;
        private Dictionary<string, ScriptsRun> _scriptsCache;
        protected IConnection<TDbConnection> AdminConnection;

        protected IConnection<TDbConnection> ServerConnection;
        protected bool SplitBatches = true;
        public string MasterDatabaseName { get; set; }
        public IRepository Repository { get; set; }
        public ConfigurationPropertyHolder configuration { get; set; }
        public string server_name { get; set; }
        public string database_name { get; set; }
        public string provider { get; set; }
        public string connection_string { get; set; }
        public string admin_connection_string { get; set; }
        public string roundhouse_schema_name { get; set; }
        public string version_table_name { get; set; }
        public string scripts_run_table_name { get; set; }
        public string scripts_run_errors_table_name { get; set; }
        public string user_name { get; set; }

        public virtual string sql_statement_separator_regex_pattern{
            get{
                return
                    @"(?<KEEP1>^(?:[\s\t])*(?:-{2}).*$)|(?<KEEP1>/{1}\*{1}[\S\s]*?\*{1}/{1})|(?<KEEP1>'{1}(?:[^']|\n[^'])*?'{1})|(?<KEEP1>^|\s)(?<BATCHSPLITTER>\;)(?<KEEP2>\s|$)";
            }
        }

        public int command_timeout { get; set; }
        public int admin_command_timeout { get; set; }
        public int restore_timeout { get; set; }

        public virtual bool split_batch_statements{
            get { return SplitBatches; }
            set { SplitBatches = value; }
        }

        public virtual bool supports_ddl_transactions{
            get { return true; }
        }

        //this method must set the provider
        public abstract void initialize_connections(ConfigurationPropertyHolder configurationPropertyHolder);

        public abstract void open_connection(bool withTransaction);
        public abstract void close_connection();
        public abstract void open_admin_connection();
        public abstract void close_admin_connection();

        public abstract void rollback();

        public virtual bool create_database_if_it_doesnt_exist(string customCreateDatabaseScript){
            var databaseWasCreated = false;
            try{
                var createScript = create_database_script();
                if (!string.IsNullOrEmpty(customCreateDatabaseScript)){
                    createScript = customCreateDatabaseScript;
                    if (!configuration.DisableTokenReplacement){
                        createScript = TokenReplacer.replace_tokens(configuration, createScript);
                    }
                }

                if (split_batch_statements){
                    foreach (
                        var sqlStatement in
                            StatementSplitter.split_sql_on_regex_and_remove_empty_statements(createScript,
                                sql_statement_separator_regex_pattern)){
                        //should only receive a return value once
                        var returnValue = run_sql_scalar_boolean(sqlStatement, ConnectionType.Admin);
                        if (returnValue != null){
                            databaseWasCreated = returnValue.Value;
                        }
                    }
                }
                else{
                    //should only receive a return value once
                    var returnValue = run_sql_scalar_boolean(createScript, ConnectionType.Admin);
                    databaseWasCreated = returnValue.GetValueOrDefault(false);
                }
            }
            catch (Exception ex){
                Log.bound_to(this).log_a_warning_event_containing(
                    "{0} with provider {1} does not provide a facility for creating a database at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
            }

            return databaseWasCreated;
        }

        public void set_recovery_mode(bool simple){
            try{
                run_sql(set_recovery_mode_script(simple), ConnectionType.Admin);
            }
            catch (Exception ex){
                Log.bound_to(this).log_a_warning_event_containing(
                    "{0} with provider {1} does not provide a facility for setting recovery mode to simple at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
            }
        }

        public void backup_database(string outputPathMinusDatabase){
            try{
                run_sql(set_backup_database_script(), ConnectionType.Admin);
            }
            catch (Exception ex){
                Log.bound_to(this).log_a_warning_event_containing(
                    "{0} with provider {1} does not provide a facility for backing up the database at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
            }
        }

        public void restore_database(string restoreFromPath, string customRestoreOptions){
            try{
                var currentConnectionTimeout = command_timeout;
                admin_command_timeout = restore_timeout;
                run_sql(restore_database_script(restoreFromPath, customRestoreOptions), ConnectionType.Admin);
                admin_command_timeout = currentConnectionTimeout;
            }
            catch (Exception ex){
                Log.bound_to(this).log_a_warning_event_containing(
                    "{0} with provider {1} does not provide a facility for restoring a database at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
            }
        }

        public virtual void delete_database_if_it_exists(){
            try{
                run_sql(delete_database_script(), ConnectionType.Admin);
            }
            catch (Exception ex){
                Log.bound_to(this).log_an_error_event_containing(
                    "{0} with provider {1} does not provide a facility for deleting a database at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
                throw;
            }
        }

        public abstract void run_database_specific_tasks();

        public virtual void create_or_update_roundhouse_tables(){
            var s = new SchemaUpdate(Repository.nhibernate_configuration);
            s.Execute(false, true);
        }

        public virtual void run_sql(string sqlToRun, ConnectionType connectionType){
            Log.bound_to(this)
                .log_a_debug_event_containing("[SQL] Running (on connection '{0}'): {1}{2}", connectionType.ToString(),
                    Environment.NewLine, sqlToRun);
            run_sql(sqlToRun, connectionType, null);
        }

        public virtual object run_sql_scalar(string sqlToRun, ConnectionType connectionType){
            Log.bound_to(this)
                .log_a_debug_event_containing("[SQL] Running (on connection '{0}'): {1}{2}", connectionType.ToString(),
                    Environment.NewLine, sqlToRun);
            return run_sql_scalar(sqlToRun, connectionType, null);
        }

        public void insert_script_run(string scriptName, string sqlToRun, string sqlToRunHash, bool runThisScriptOnce,
            long versionId){
            var scriptRun = new ScriptsRun{
                version_id = versionId,
                script_name = scriptName,
                text_of_script = sqlToRun,
                text_hash = sqlToRunHash,
                one_time_script = runThisScriptOnce
            };

            try{
                Repository.save_or_update(scriptRun);
            }
            catch (Exception ex){
                Log.bound_to(this).log_an_error_event_containing(
                    "{0} with provider {1} does not provide a facility for recording scripts run at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
                throw;
            }
        }

        public void insert_script_run_error(string scriptName, string sqlToRun, string sqlErroneousPart,
            string errorMessage, string repositoryVersion,
            string repositoryPath){
            var scriptRunError = new ScriptsRunError{
                version = repositoryVersion ?? string.Empty,
                script_name = scriptName,
                text_of_script = sqlToRun,
                erroneous_part_of_script = sqlErroneousPart,
                repository_path = repositoryPath ?? string.Empty,
                error_message = errorMessage
            };

            try{
                Repository.save_or_update(scriptRunError);
            }
            catch (Exception ex){
                Log.bound_to(this).log_an_error_event_containing(
                    "{0} with provider {1} does not provide a facility for recording scripts run errors at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
                throw;
            }
        }

        public string get_version(string repositoryPath){
            var version = "0";

            var crit = QueryOver.Of<Version>()
                .Where(x => x.repository_path == (repositoryPath ?? string.Empty))
                .OrderBy(x => x.entry_date).Desc
                .Take(1);

            try{
                var items = Repository.get_with_criteria(crit);
                if (items != null && items.Count > 0){
                    version = items[0].version;
                }
            }
            catch (Exception ex){
                Log.bound_to(this)
                    .log_a_warning_event_containing(
                        "{0} with provider {1} does not provide a facility for retrieving versions at this time.{2}{3}",
                        GetType(), provider, Environment.NewLine, ex.to_string());
            }

            return version;
        }

        //get rid of the virtual
        public virtual long insert_version_and_get_version_id(string repositoryPath, string repositoryVersion){
            long versionId;

            var version = new Version{
                version = repositoryVersion ?? string.Empty,
                repository_path = repositoryPath ?? string.Empty
            };

            try{
                Repository.save_or_update(version);
                versionId = version.id;
            }
            catch (Exception ex){
                Log.bound_to(this)
                    .log_an_error_event_containing(
                        "{0} with provider {1} does not provide a facility for inserting versions at this time.{2}{3}",
                        GetType(), provider, Environment.NewLine, ex.Message);
                throw;
            }

            return versionId;
        }

        public string get_current_script_hash(string scriptName){
            var script = get_from_script_cache(scriptName);

            if (script != null){
                return script.text_hash;
            }

            var crit = QueryOver.Of<ScriptsRun>()
                .Where(x => x.script_name == scriptName)
                .OrderBy(x => x.id).Desc
                .Take(1);

            var hash = string.Empty;

            try{
                var items = Repository.get_with_criteria(crit);
                if (items != null && items.Count > 0){
                    hash = items[0].text_hash;
                }
            }
            catch (Exception ex){
                Log.bound_to(this).log_an_error_event_containing(
                    "{0} with provider {1} does not provide a facility for hashing (through recording scripts run) at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
                throw;
            }

            return hash;
        }

        public bool has_run_script_already(string scriptName){
            var script = get_from_script_cache(scriptName);

            if (script != null){
                return true;
            }

            var scriptHasRun = false;

            var crit = QueryOver.Of<ScriptsRun>()
                .Where(x => x.script_name == scriptName)
                .OrderBy(x => x.id).Desc
                .Take(1);

            try{
                var items = Repository.get_with_criteria(crit);
                if (items != null && items.Count > 0){
                    scriptHasRun = true;
                }
            }
            catch (Exception ex){
                Log.bound_to(this).log_an_error_event_containing(
                    "{0} with provider {1} does not provide a facility for determining if a script has run at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
                throw;
            }

            return scriptHasRun;
        }

        public virtual void Dispose(){
            if (!_disposing){
                Log.bound_to(this).log_a_debug_event_containing("Database is disposing normal connection.");
                dispose_connection(ServerConnection);
                Log.bound_to(this).log_a_debug_event_containing("Database is disposing admin connection.");
                dispose_connection(AdminConnection);

                _disposing = true;
            }
        }

        public abstract void set_provider();

        public void set_repository(){
            var sessionFactoryBuilder = new NHibernateSessionFactoryBuilder(configuration);
            Configuration cfg = null;
            var factory = sessionFactoryBuilder.build_session_factory(x => { cfg = x; });
            Repository = new Repository(factory, cfg);
        }

        public abstract string create_database_script();
        public abstract string set_recovery_mode_script(bool simple);
        public abstract string restore_database_script(string restoreFromPath, string customRestoreOptions);
        public abstract string delete_database_script();
        public abstract string set_backup_database_script();

        private bool? run_sql_scalar_boolean(string sqlToRun, ConnectionType connectionType){
            var returnValue = run_sql_scalar(sqlToRun, connectionType);
            if (returnValue != null && returnValue != DBNull.Value){
                return Convert.ToBoolean(returnValue);
            }
            return null;
        }

        protected abstract void run_sql(string sqlToRun, ConnectionType connectionType,
            IList<IParameter<IDbDataParameter>> parameters);

        protected abstract object run_sql_scalar(string sqlToRun, ConnectionType connectionType,
            IList<IParameter<IDbDataParameter>> parameters);

        private void dispose_connection(IConnection<TDbConnection> connection){
            if (connection != null){
                var conn = (IDbConnection) connection.underlying_type();
                if (conn != null){
                    if (conn.State != ConnectionState.Closed){
                        conn.Close();
                    }
                }

                connection.Dispose();
            }
        }

        private ScriptsRun get_from_script_cache(string scriptName){
            ensure_script_cache();

            ScriptsRun script;
            if (_scriptsCache.TryGetValue(scriptName, out script)){
                return script;
            }

            return null;
        }

        private void ensure_script_cache(){
            if (_scriptsCache != null){
                return;
            }

            _scriptsCache = new Dictionary<string, ScriptsRun>();

            // latest id overrides possible old one, just like in queries searching for scripts
            foreach (var script in Repository.get_all<ScriptsRun>().OrderBy(x => x.id)){
                _scriptsCache[script.script_name] = script;
            }
        }
    }
}