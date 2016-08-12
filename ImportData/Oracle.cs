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
    public class Oracle
    {
        string baseDir = System.AppDomain.CurrentDomain.BaseDirectory;//当前执行目录

        #region 1.0

        public void CreateOracleTrigger(string tableName)
        {
            List<ColumnInfo> columnInfoList = GetColumnList(tableName);
            if (columnInfoList == null) return;

            StringBuilder sb = new StringBuilder("'insert into " + tableName.ToUpper() + " values('");
            //StringBuilder sb = new StringBuilder("'concat(insert into " + tableName.ToUpper() + " values('");
            columnInfoList.ForEach(u =>
            {

                //if (u.ColumnType == "DATE")
                //{
                //    sb.Append("||'ifnull(to_char('||:NEW." + u.ColumnName + "||',''yyyy-mm-dd hh24:mi:ss'')'||','||'null'||')'||','");
                //}
                //else
                //{
                //    sb.Append("||'ifnull('||''''||:NEW." + u.ColumnName + "||''''||','||'null'||')'||','");
                //}

                //if (u.ColumnType == "DATE")
                //{
                //    sb.Append("||'nvl(to_date(to_char('||:NEW." + u.ColumnName + "||',''yyyy-mm-dd hh24:mi:ss'')'||',''yyyy-mm-dd hh24:mi:ss'')'||','||'null'||')'||','");
                //}
                //else
                //{
                //    sb.Append("||'nvl('||''''||:NEW." + u.ColumnName + "||''''||','||'null'||')'||','");
                //}

                //if (u.ColumnType == "DATE")
                //{
                //    sb.Append(",'nvl(to_char(',:NEW." + u.ColumnName + ",',''yyyy-mm-dd hh24:mi:ss'')',',','null',')',','");
                //}
                //else
                //{
                //    sb.Append(",'nvl(','''',:NEW." + u.ColumnName + ",'''',',','null',')',','");
                //}



            });
            //string columnStr = sb.ToString().Substring(0,sb.Length-3) + "')'";
            string columnStr = sb.ToString().Substring(0, sb.Length - 3) + "'))'";
            string sql = @"create or replace trigger sync_insert_test
                            before update
                            on {0}
                            for each row
                            declare insert_sql varchar(2000);
                            begin
                                insert_sql:={1};
                                insert into cx_data_trigger_sync_test(id,command) values(S_CX_DATA_TRIGGER_SYNC_TEST.NEXTVAL,insert_sql);
                            end;";
            sql = string.Format(sql, tableName, columnStr);
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public void CreateOracleInsertTrigger(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbValue = new StringBuilder("");
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbType = new StringBuilder();

            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");

                if (u.ColumnType.IndexOf("DATE") != -1)
                {
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + ",'yyyy-mm-dd hh24:mi:ss'),null)||''','||");
                }
                else
                {
                    sbValue.Append("''''||nvl(:NEW." + u.ColumnName + ",null)||''','||");
                }


                sbType.Append("''" + u.ColumnType + "'',");
            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 4) + "'";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);

            string sql = @"create or replace trigger {0}_insert
                            before insert
                            on {0}
                            for each row
                            declare insert_sql varchar(2000);
                            begin
                                insert_sql:='{{'||'''ColumnList'':'||'[{1}]'||',''ValueList'':'||'['||{2}||']'||',''TypeList'':'||'[{3}]'||',''TableName'':''{0}'''||',''Where'':'||''''''||',''OperateType'':'||'''insert'''||'}}';
                                insert into DATA_CHANGE_COMMAND_SEND(id,PARAMETERS) values(S_DATA_CHANGE_COMMAND_SEND.NEXTVAL,insert_sql);
                            end;";
            sql = string.Format(sql, tableName, columnStr, valueStr, typeStr);
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public void CreateOracleUpdateTrigger(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbValue = new StringBuilder("");
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbType = new StringBuilder();
            string where = "";

            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbType.Append("''" + u.ColumnType + "'',");

                if (u.ColumnType.IndexOf("DATE") != -1)
                {
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + ",'yyyy-mm-dd hh24:mi:ss'),null)||''','||");
                }
                else
                {
                    sbValue.Append("''''||nvl(:NEW." + u.ColumnName + ",null)||''','||");
                }

                if (u.IsKey)
                {
                    where = "'" + u.ColumnName + "'||'='||'\"'||" + ":old." + u.ColumnName + "||'\"'";
                }

            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 4) + "'";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);

            string sql = @"create or replace trigger {3}_update
                            before update
                            on {3}
                            for each row
                            declare insert_sql varchar(2000);
                            begin
                                insert_sql:='{{'||'''ColumnList'':'||'[{0}]'||',''ValueList'':'||'['||{1}||']'||',''TypeList'':'||'[{2}]'||',''TableName'':''{3}'''||',''Where'':'||''''||{4}||''''||',''OperateType'':'||'''update'''||'}}';
                                insert into DATA_CHANGE_COMMAND_SEND(id,PARAMETERS) values(S_DATA_CHANGE_COMMAND_SEND.NEXTVAL,insert_sql);
                            end;";
            sql = string.Format(sql, columnStr, valueStr, typeStr, tableName, where);
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public void CreateOracleDeleteTrigger(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbValue = new StringBuilder("");
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbType = new StringBuilder();
            string where = "";
            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbType.Append("''" + u.ColumnType + "'',");

                if (u.ColumnType.IndexOf("DATE") != -1)
                {
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + ",'yyyy-mm-dd hh24:mi:ss'),null)||''','||");
                }
                else
                {
                    sbValue.Append("''''||nvl(:NEW." + u.ColumnName + ",null)||''','||");
                }

                if (u.IsKey)
                {
                    where = "'" + u.ColumnName + "'||'='||'\"'||" + ":old." + u.ColumnName + "||'\"'";
                }
            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 4) + "'";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);

            string sql = @"create or replace trigger {0}_delete
                            before delete
                            on {0}
                            for each row
                            declare insert_sql varchar(2000);
                            begin
                                insert_sql:='{{'||'''ColumnList'':'||'[{1}]'||',''ValueList'':'||'['||{2}||']'||',''TypeList'':'||'[{3}]'||',''TableName'':''{0}'''||',''Where'':'||''''||{4}||''''||',''OperateType'':'||'''delete'''||'}}';
                                insert into DATA_CHANGE_COMMAND_SEND(id,PARAMETERS) values(S_DATA_CHANGE_COMMAND_SEND.NEXTVAL,insert_sql);
                            end;";
            sql = string.Format(sql, tableName, columnStr, valueStr, typeStr, where);
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        } 
        #endregion

        #region 2.0
        public void NewCreateOracleInsertTrigger(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbValue = new StringBuilder("");
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbType = new StringBuilder();

            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");

                if (u.ColumnType.IndexOf("DATE") != -1)
                {
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + ",'yyyy-mm-dd hh24:mi:ss'),'null')||''','||");
                }
                else
                {
                    //sbValue.Append("''''||nvl(:NEW." + u.ColumnName + ",'null')||''','||");
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + "),'null')||''','||");
                }


                sbType.Append("''" + u.ColumnType + "'',");
            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 4) + "'";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);

            string sql = @"create or replace trigger {0}_insert
                            before insert
                            on {0}
                            for each row
                            declare insert_sql varchar(2000);
                            begin
                                if :NEW.LASTUPDATEDBSOURCE='1' then
                                    insert_sql:='{{'||'''ColumnList'':'||'[{1}]'||',''ValueList'':'||'['||{2}||']'||',''TypeList'':'||'[{3}]'||',''TableName'':''{0}'''||',''PkCloumn'':'||''''''||',''OperateType'':'||'''insert'''||'}}';
                                    insert into DATA_CHANGE_COMMAND_SEND(id,PARAMETERS) values(S_DATA_CHANGE_COMMAND_SEND.NEXTVAL,insert_sql);
                                else
                                    :NEW.LASTUPDATEDBSOURCE:='1';
                                end if;
                            end;";
            sql = string.Format(sql, tableName, columnStr, valueStr, typeStr);
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public void NewCreateOracleUpdateTrigger(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbValue = new StringBuilder("");
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbType = new StringBuilder();
            string pkCloumn = "";

            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbType.Append("''" + u.ColumnType + "'',");

                if (u.ColumnType.IndexOf("DATE") != -1)
                {
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + ",'yyyy-mm-dd hh24:mi:ss'),'null')||''','||");
                }
                else
                {
                   // sbValue.Append("''''||nvl(:NEW." + u.ColumnName + ",'null')||''','||");
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + "),'null')||''','||");
                }

                if (u.IsKey)
                {
                    //where = "'" + u.ColumnName + "'||'='||'\"'||" + ":old." + u.ColumnName + "||'\"'";
                    //where = "\"" + u.ColumnName + "='" + ":old." + u.ColumnName + "'\"";
                    pkCloumn = u.ColumnName;
                }

            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 4) + "'";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);

            string sql = @"create or replace trigger {3}_update
                            before update
                            on {3}
                            for each row
                            declare insert_sql varchar(2000);
                            begin
                                if :NEW.LASTUPDATEDBSOURCE='1' then
                                    insert_sql:='{{'||'''ColumnList'':'||'[{0}]'||',''ValueList'':'||'['||{1}||']'||',''TypeList'':'||'[{2}]'||',''TableName'':''{3}'''||',''PkCloumn'':'||'''{4}'''||',''OperateType'':'||'''update'''||'}}';
                                    insert into DATA_CHANGE_COMMAND_SEND(id,PARAMETERS) values(S_DATA_CHANGE_COMMAND_SEND.NEXTVAL,insert_sql);
                                elsif :NEW.LASTUPDATEDBSOURCE='0' then
                                    :NEW.LASTUPDATEDBSOURCE:='1';
                                end if;
                            end;";
            sql = string.Format(sql, columnStr, valueStr, typeStr, tableName, pkCloumn);
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public void NewCreateOracleDeleteTrigger(string tableName)
        {
            List<ColumnInfo> columnList = GetColumnList(tableName);
            if (columnList == null) return;

            StringBuilder sbValue = new StringBuilder("");
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbType = new StringBuilder();
            string pkCloumn = "";
            columnList.ForEach(u =>
            {
                sbColumn.Append("''" + u.ColumnName + "'',");
                sbType.Append("''" + u.ColumnType + "'',");

                if (u.ColumnType.IndexOf("DATE") != -1)
                {
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + ",'yyyy-mm-dd hh24:mi:ss'),'null')||''','||");
                }
                else
                {
                    //sbValue.Append("''''||nvl(:NEW." + u.ColumnName + ",'null')||''','||");
                    sbValue.Append("''''||nvl(to_char(:NEW." + u.ColumnName + "),'null')||''','||");
                }

                if (u.IsKey)
                {
                    //where = "\"" + u.ColumnName + "='" + ":old." + u.ColumnName + "'\"";
                    pkCloumn = u.ColumnName;
                }
            });
            string columnStr = sbColumn.ToString().Substring(0, sbColumn.Length - 1);
            string valueStr = sbValue.ToString().Substring(0, sbValue.Length - 4) + "'";
            string typeStr = sbType.ToString().Substring(0, sbType.Length - 1);

            string sql = @"create or replace trigger {0}_delete
                            before delete
                            on {0}
                            for each row
                            declare insert_sql varchar(2000);
                            begin
                                if :OLD.LASTUPDATEDBSOURCE='1' then
                                    insert_sql:='{{'||'''ColumnList'':'||'[{1}]'||',''ValueList'':'||'['||{2}||']'||',''TypeList'':'||'[{3}]'||',''TableName'':''{0}'''||',''PkCloumn'':'||'''{4}'''||',''OperateType'':'||'''delete'''||'}}';
                                    insert into DATA_CHANGE_COMMAND_SEND(id,PARAMETERS) values(S_DATA_CHANGE_COMMAND_SEND.NEXTVAL,insert_sql);
                                end if;
                            end;";
            sql = string.Format(sql, tableName, columnStr, valueStr, typeStr, pkCloumn);
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
        #endregion

        public void AddColumn(string tableName) 
        {
            string sql = "alter   table  " + tableName + "   add( LastUpdateDbSource  char(1) default '1' not null)";
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public List<ColumnInfo> GetColumnList(string tableName) 
        {
            List<ColumnInfo> columnList = null;
            string sql = @"select a.column_name Field ,data_type Type, case  when b.column_name=a.column_name then 'PRI' else '' end Key from user_tab_columns a
                            left join
                            (
                                select   *   from    user_cons_columns   
                                where    constraint_name   =    (select    constraint_name   from    user_constraints   
                                where    table_name   =   '{0}'  and    constraint_type   ='P')
                            ) b
                            on a.column_name=b.column_name
                             where a.table_name='{0}'";
            sql = string.Format(sql, tableName.ToUpper());

            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            DataSet ds = new DataSet();
            OracleDataAdapter adapter = new OracleDataAdapter(cmd);
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

        public List<string> GetTableName() 
        {
            List<string> tableList = null;
            string sql = "SELECT ts.TABLE_NAME FROM USER_TABLES ts";
            OracleCommand cmd = OpenOracleConn().CreateCommand();
            cmd.CommandText = sql;
            DataSet ds = new DataSet();
            OracleDataAdapter adapter = new OracleDataAdapter(cmd);
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
                CloseConn(OpenOracleConn());
            }
            return tableList;
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


    public class ColumnInfo 
    {
        /// <summary>
        /// 列名
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// 列类型
        /// </summary>
        public string ColumnType { get; set; }

        /// <summary>
        /// 是否是主键
        /// </summary>
        public bool IsKey { get; set; }
    }
}
