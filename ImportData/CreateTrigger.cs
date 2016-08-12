using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.IO;
using System.Reflection;
using MySql.Data.MySqlClient;
using System.Data.Common;
using System.Threading;

namespace ImportData
{
    public class CreateTrigger
    {
        string baseDir = System.AppDomain.CurrentDomain.BaseDirectory;//当前执行目录

        #region MyRegion
        public void CreateMysqlTriggerInsert(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbValue = new StringBuilder("concat(");
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbType = new StringBuilder();
            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbValue.Append("'''',ifnull(new." + u.ColumnName + ",'null'),''',',");
                sbType.Append("''" + u.ColumnType + "'',");
            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 3) + "')";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);
            string sql = @"DROP trigger
                            IF EXISTS sync_insert;
                            create trigger sync_insert
                            before insert
                            on {3}
                            for each row
                            begin
                                declare insert_sql varchar(2000);
                                set insert_sql:=concat('{{','''ColumnList'':','[{0}]',',''ValueList'':','[',{1},']',',''TypeList'':','[{2}]',',''TableName'':''{3}''',',''Where'':','''''',',''OperateType'':','''insert''','}}');
                                insert into data_change_command_send(PARAMETERS) values(insert_sql);                           
                            end";
            sql = string.Format(sql, columnStr, valueStr, typeStr, tableName);
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();

        }

        public void CreateMysqlTriggerUpdate(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbValue = new StringBuilder("concat(");
            StringBuilder sbType = new StringBuilder();
            string where = "";
            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbValue.Append("'''',ifnull(new." + u.ColumnName + ",'null'),''',',");
                sbType.Append("''" + u.ColumnType + "'',");
                if (u.IsKey)
                {
                    where = "concat('" + u.ColumnName + "','=','\"'," + "old." + u.ColumnName + ",'\"')";
                    //where = u.ColumnName + "='" + ":old." + u.ColumnName+"'";
                }
            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 3) + "')";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);
            string sql = @"DROP trigger
                            IF EXISTS sync_update;
                            create trigger sync_update
                            before update
                            on {3}
                            for each row
                            begin
                                declare insert_sql varchar(2000);
                                set insert_sql:=concat('{{','''ColumnList'':','[{0}]',',''ValueList'':','[',{1},']',',''TypeList'':','[{2}]',',''TableName'':''{3}''',',''Where'':','''',{4},'''',',''OperateType'':','''update''','}}');
                                insert into data_change_command_send(PARAMETERS) values(insert_sql);                             
                            end";
            sql = string.Format(sql, columnStr, valueStr, typeStr, tableName, where);
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public void CreateMysqlTriggerDelete(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbValue = new StringBuilder("concat(");
            StringBuilder sbType = new StringBuilder();
            string where = "";
            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbValue.Append("'''',ifnull(old." + u.ColumnName + ",'null'),''',',");
                sbType.Append("''" + u.ColumnType + "'',");
                if (u.IsKey)
                {
                    where = "concat('" + u.ColumnName + "','=','\"'," + "old." + u.ColumnName + ",'\"')";

                    //where = u.ColumnName + "='" + ":old." + u.ColumnName + "'";
                }
            });
            if (string.IsNullOrEmpty(where))
            {
                Console.Write("表【" + tableName + "】没有主键，请先建立主键");
                return;
            }

            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 3) + "')";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);
            string sql = @"DROP trigger
                            IF EXISTS sync_delete;
                            create trigger sync_delete
                            before delete
                            on {3}
                            for each row
                            begin
                                declare insert_sql varchar(2000);
                                set insert_sql:=concat('{{','''ColumnList'':','[{0}]',',''ValueList'':','[',{1},']',',''TypeList'':','[{2}]',',''TableName'':''{3}''',',''Where'':','''',{4},'''',',''OperateType'':','''delete''','}}');
                                insert into data_change_command_send(PARAMETERS) values(insert_sql);                             
                            end";
            sql = string.Format(sql, columnStr, valueStr, typeStr, tableName, where);
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
        
        #endregion

        #region 2.0
        public void NewCreateMysqlTriggerInsert(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbValue = new StringBuilder("concat(");
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbType = new StringBuilder();
            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbValue.Append("'''',ifnull(new." + u.ColumnName + ",'null'),''',',");
                sbType.Append("''" + u.ColumnType + "'',");
            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 3) + "')";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);
            string sql = @"DROP trigger
                            IF EXISTS sync_insert;
                            create trigger sync_insert
                            before insert
                            on {3}
                            for each row
                            begin  
                                declare insert_sql varchar(2000);
                                if new.LastUpdateDbSource='0' then
                                    set insert_sql:=concat('{{','''ColumnList'':','[{0}]',',''ValueList'':','[',{1},']',',''TypeList'':','[{2}]',',''TableName'':''{3}''',',''PkCloumn'':','''''',',''OperateType'':','''insert''','}}');
                                    insert into data_change_command_send(PARAMETERS) values(insert_sql); 
                                ELSE
                                    set new.LastUpdateDbSource='0' ; 
                                end if;                          
                            end";
            sql = string.Format(sql, columnStr, valueStr, typeStr, tableName);
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();

        }

        public void NewCreateMysqlTriggerUpdate(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbValue = new StringBuilder("concat(");
            StringBuilder sbType = new StringBuilder();
            string pkCloumn = "";
            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbValue.Append("'''',ifnull(new." + u.ColumnName + ",'null'),''',',");
                sbType.Append("''" + u.ColumnType + "'',");
                if (u.IsKey)
                {
                    //where = "concat('" + u.ColumnName + "','=','\"'," + "old." + u.ColumnName + ",'\"')";
                    //where = "concat('" + u.ColumnName + "','=','\"'," + "old." + u.ColumnName + ",'\"')";
                    pkCloumn = u.ColumnName;
                }
            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 3) + "')";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);
            string sql = @"DROP trigger
                            IF EXISTS sync_update;
                            create trigger sync_update
                            before update
                            on {3}
                            for each row
                            begin
                                declare insert_sql varchar(2000);
                                if new.LastUpdateDbSource='0' then
                                    set insert_sql:=concat('{{','''ColumnList'':','[{0}]',',''ValueList'':','[',{1},']',',''TypeList'':','[{2}]',',''TableName'':''{3}''',',''PkCloumn'':','''{4}''',',''OperateType'':','''update''','}}');
                                    insert into data_change_command_send(PARAMETERS) values(insert_sql);
                                elseif new.LastUpdateDbSource='1' then
                                    SET new.LastUpdateDbSource='0';
                                end if;                          
                            end";
            sql = string.Format(sql, columnStr, valueStr, typeStr, tableName, pkCloumn);
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public void NewCreateMysqlTriggerDelete(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbValue = new StringBuilder("concat(");
            StringBuilder sbType = new StringBuilder();
            string pkCloumn = "";
            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbValue.Append("'''',ifnull(old." + u.ColumnName + ",'null'),''',',");
                sbType.Append("''" + u.ColumnType + "'',");
                if (u.IsKey)
                {
                    //where = "concat('" + u.ColumnName + "','=','\"'," + "old." + u.ColumnName + ",'\"')";
                    pkCloumn = u.ColumnName;
                    //where = u.ColumnName + "='" + ":old." + u.ColumnName + "'";
                }
            });
            if (string.IsNullOrEmpty(pkCloumn))
            {
                Console.Write("表【" + tableName + "】没有主键，请先建立主键");
                return;
            }

            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 3) + "')";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);
            string sql = @"DROP trigger
                            IF EXISTS sync_delete;
                            create trigger sync_delete
                            before delete
                            on {3}
                            for each row
                            begin
                                declare insert_sql varchar(2000);
                                if OLD.LASTUPDATEDBSOURCE='0' then
                                    set insert_sql:=concat('{{','''ColumnList'':','[{0}]',',''ValueList'':','[',{1},']',',''TypeList'':','[{2}]',',''TableName'':''{3}''',',''PkCloumn'':','''{4}''',',''OperateType'':','''delete''','}}');
                                    insert into data_change_command_send(PARAMETERS) values(insert_sql);                             
                                end if;
                            end";
            sql = string.Format(sql, columnStr, valueStr, typeStr, tableName, pkCloumn);
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
        #endregion

        public void AddColumn(string tableName) 
        {
            string sql = "alter table " + tableName + " add LastUpdateDbSource char(1) not Null default '0'";
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public List<string> GetTableName (string databaseName)
        {
            List<string> tableList = null;
            string sql = "select table_name from information_schema.tables where table_schema='" + databaseName + "' and table_type='base table';";
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            DataSet ds = new DataSet();
            MySqlDataAdapter adapter = new MySqlDataAdapter(cmd);
            try
            {
                adapter.Fill(ds);
                if (ds.Tables.Count > 0)
                {
                    tableList = new List<string>();
                    int rowCount = ds.Tables[0].Rows.Count;
                    for (int i = 0; i < rowCount; i++)
                    {
                        tableList.Add(ds.Tables[0].Rows[i]["table_name"].ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                Write(ex.Message, "getTableErr");
            }
            finally
            {
                CloseConn(OpenMySqlConn());
            }
            return tableList;
        }

        public List<ColumnInfo> GetColumnList(string tableName)
        {
            List<ColumnInfo> columnList = null;
            string sql = "show columns from " + tableName;
            MySqlCommand cmd = OpenMySqlConn().CreateCommand();
            cmd.CommandText = sql;
            DataSet ds = new DataSet();
            MySqlDataAdapter adapter = new MySqlDataAdapter(cmd);
            try
            {
                adapter.Fill(ds);
                if (ds.Tables.Count > 0)
                {
                    columnList = new List<ColumnInfo>();
                    int rowCount = ds.Tables[0].Rows.Count;
                    for (int i = 0; i < rowCount; i++)
                    {
                        ColumnInfo info = new ColumnInfo();
                        info.ColumnName = ds.Tables[0].Rows[i]["Field"].ToString();
                        info.ColumnType = ds.Tables[0].Rows[i]["Type"].ToString();
                        info.IsKey = ds.Tables[0].Rows[i]["Key"].ToString().ToUpper()=="PRI"?true:false;
                        columnList.Add(info);
                    }
                }
            }
            catch (Exception ex)
            {
                Write(ex.Message, "getColumnErr");
            }
            finally 
            {
                CloseConn(OpenMySqlConn());
            }
            return columnList;
        }


        /// <summary>
        /// 打开Orcacle数据库连接
        /// </summary>
        /// <returns></returns>
        OracleConnection OpenOracleConn()
        {
            OracleConnection conn = new OracleConnection();
            conn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["SourceOrcal"].ToString();
            conn.Open();
            return conn;
        }

        /// <summary>
        /// 打开mysql数据库连接
        /// </summary>
        /// <returns></returns>
        MySqlConnection OpenMySqlConn()
        {
            MySqlConnection conn = new MySqlConnection();
            conn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["ImportMySql"].ToString();
            conn.Open();
            return conn;
        }

        /// <summary>
        /// 关闭数据库连接
        /// </summary>
        /// <param name="conn"></param>
        void CloseConn(IDbConnection conn)
        {
            if (conn == null) { return; }
            try
            {
                if (conn.State != ConnectionState.Closed)
                {
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                conn.Dispose();
            }
        }

        void Write(string longStr, string textName = "ErrorLog")
        {
            StreamWriter sw = new StreamWriter(baseDir + "\\" + textName + ".txt", true);
            sw.WriteLine(longStr);
            sw.Close();//写入
        }
    }


    public class SqlCommand 
    {
        public List<string> columnList { get; set; }

        public List<string> columnValue { get; set; }

        public List<string> columnType { get; set; }

        public string Operate { get; set; }

        public string where { get; set; }
    }
  
}
