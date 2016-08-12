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
    class Program
    {

        static object obj = new object();//锁
        static object curobj = new object();//锁
        static object curthreadobj = new object();//锁
        static string baseDir = System.AppDomain.CurrentDomain.BaseDirectory;//当前执行目录
        static int curIndex = 0;//当前表数
        static int totalIndex = 0;//总表数
        static int curthread = 0;//当前线程数
        static int threadCount = 0;//总线程数
        static DateTime startDate = DateTime.Now;//开始同步时间
        static DateTime endDate = default(DateTime);//结束同步时间

        /// <summary>
        /// 单线程同步数据量
        /// </summary>
        public static long ThreadDatavolume
        {
            get
            {
                long value = 0;
                string valuestr = System.Configuration.ConfigurationManager.AppSettings["ThreadDatavolume"];
                if (
                    string.IsNullOrEmpty(valuestr)
                    ||
                    !long.TryParse(valuestr, out value)
                  )
                {
                    throw new Exception("ThreadDatavolume配置值有错误");
                }
                return value;
            }
        }

        /// <summary>
        /// 单次读取数据量
        /// </summary>
        public static long SingleDatavolume
        {
            get
            {
                long value = 0;
                string valuestr = System.Configuration.ConfigurationManager.AppSettings["SingleDatavolume"];
                if (
                    string.IsNullOrEmpty(valuestr)
                    ||
                    !long.TryParse(valuestr, out value)
                  )
                {
                    throw new Exception("SingleDatavolume配置值有错误");
                }
                return value;
            }

        }

        static List<SingleImportData> SingleImportDataList = new List<SingleImportData>();

        /// <summary>
        /// 按数据量分配的数据对象
        /// </summary>
        static List<ImportDataConfig> ImportDataConfigList = new List<ImportDataConfig>();

        static void Main(string[] args)
        {

            CreateTrigger trigger = new CreateTrigger();
            //trigger.AddColumn("cx_aa_test");
            trigger.NewCreateMysqlTriggerInsert("cx_aa_test");
            trigger.NewCreateMysqlTriggerUpdate("cx_aa_test");
            trigger.NewCreateMysqlTriggerDelete("cx_aa_test");

            Oracle oracle = new Oracle();
            oracle.NewCreateOracleInsertTrigger("cx_aa_test");
            oracle.NewCreateOracleUpdateTrigger("cx_aa_test");
            oracle.NewCreateOracleDeleteTrigger("cx_aa_test");
            //oracle.AddColumn("cx_aa_test");
            return;

            Console.WriteLine(string.Format("{0}-StartDate/EndDate:{1} / {2}",
                               DateTime.Now, startDate, endDate
                                )
                  );

            #region 读取表名
            List<TableAttribute> tableList = new List<TableAttribute> { new TableAttribute("sequence", 195) };
            //using (OracleConnection oracleConn = OpenOracleConn())
            //{
            //    #region 读取表名
            //    DataTable tableNames = new DataTable();
            //    OracleCommand cmd = oracleConn.CreateCommand();
            //    cmd.CommandText = "SELECT ts.TABLE_NAME FROM USER_TABLES ts Where ts.TABLE_NAME!='CX_PERSONNEL_RESUME_HISTORY'  Order by ts.TABLE_NAME ";
            //    //cmd.CommandText = "SELECT ts.TABLE_NAME FROM USER_TABLES ts  Order by ts.TABLE_NAME ";
            //    cmd.CommandType = CommandType.Text;

            //    OracleDataAdapter oracelAdapter = new OracleDataAdapter();
            //    oracelAdapter.SelectCommand = cmd;
            //    oracelAdapter.Fill(tableNames);
            //    #endregion

            //    #region  查询总条数
            //    if (!(tableNames != null
            //          &&
            //          tableNames.Rows.Count > 0
            //         )
            //        )
            //    {
            //        return;
            //    }


            //    for (int i = 102; i < tableNames.Rows.Count; i++)
            //    {
            //        try
            //        {
            //            cmd.CommandText = "Select Count(1) From " + tableNames.Rows[i]["TABLE_NAME"].ToString();
            //            cmd.CommandType = CommandType.Text;
            //            tableList.Add(new TableAttribute(tableNames.Rows[i]["TABLE_NAME"].ToString(), Convert.ToInt64(cmd.ExecuteScalar())));   
            //        }
            //        catch (Exception ex)
            //        {
            //            Write(tableNames.Rows[i]["TABLE_NAME"].ToString() + ex.Message);
            //        }
            //    }

            //    #endregion

            //}
            #endregion

            #region 导入数据

            totalIndex = tableList.Count;

            FileTableByDataVolume(tableList);

            ThreadPool.SetMaxThreads(50, 50);

            for (int i = 0; i < ImportDataConfigList.Count; i++)//遍历分类
            {
                ImportData(new ImportDataParameter(GetSingleImportData(ref i), OpenOracleConn(), OpenMySqlConn()));
                //ThreadPool.QueueUserWorkItem
                //(
                //  new WaitCallback(ImportData),
                //  new ImportDataParameter(GetSingleImportData(ref i), OpenOracleConn(), OpenMySqlConn())
                // );

                //threadCount++;
                //Thread.Sleep(10000);
                Console.WriteLine("ThreadCount:" + threadCount);

            }

            #endregion

            #region 保持主线程;检测线程使用率
           // keepLive();
            #endregion
        }

        /// <summary>
        /// 导入数据
        /// </summary>
        /// <param name="obj"></param>
        static void ImportData(object obj)
        {
            try
            {
                ImportDataParameter impDataP = (ImportDataParameter)obj;
                DateTime beginDate = default(DateTime);
                DateTime endDate = default(DateTime);

                #region 导入

                OracleCommand cmd = impDataP.oracleConn.CreateCommand();
                MySqlCommand mySqlcmd = impDataP.mySqlConn.CreateCommand();

                for (int p = 0; p < impDataP.SingleImportData.ImportDataList.Count; p++)
                {
                    ImportDataConfig importDataConfig = impDataP.SingleImportData.ImportDataList[p];

                    #region 查询总数
                    string tableName = importDataConfig.TableName;
                    long count = importDataConfig.Count;
                    beginDate = DateTime.Now;
                    //Console.WriteLine(string.Format("Table:{0}->BeginImport:{1}", tableName, beginDate));

                    Write(importDataConfig.MinSize + "-" + importDataConfig.MaxSize + "-" + tableName, "TableNameIndex");
                    Write(tableName, "TableName");
                    #endregion

                    #region 读取数据
                    DataTable dataTable = new DataTable();
                    cmd.CommandText = string.Format("Select * From (Select rownum rn,b.* From {0} b  Order by RowId) a  Where rn>={1} And rn<{2} ", tableName, importDataConfig.MinSize, importDataConfig.MaxSize);
                    cmd.CommandType = CommandType.Text;
                    OracleDataAdapter oAdapter = new OracleDataAdapter();
                    oAdapter.SelectCommand = cmd;
                    oAdapter.Fill(dataTable);

                    #endregion

                    #region 定义变量
                    List<string> colList = new List<string>();
                    StringBuilder insertSql = new StringBuilder();
                    StringBuilder insertParValues = new StringBuilder();
                    StringBuilder insertValues = new StringBuilder();
                    insertSql.AppendFormat("Insert Into {0} ( ", tableName);

                    if (!
                        (
                        dataTable != null
                        &&
                        dataTable.Rows.Count > 0
                        )
                        )
                    {
                        continue;
                        //break;
                    }
                    #endregion

                    #region 组织Insert Columns
                    foreach (DataColumn cls in dataTable.Columns)
                    {
                        if (cls.ColumnName.ToLower() != "rn")
                            colList.Add(cls.ColumnName);
                    }

                    insertSql.Append(string.Join(",", colList.ToArray()));
                    insertSql.Append(" )  values  ");

                    #endregion

                    #region 组织Insert Values

                    string colName = string.Empty;
                    object value = string.Empty;
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {

                        insertParValues.Append(" (");
                        insertValues.Append(" (");
                        for (int j = 0; j < colList.Count; j++)
                        {
                            #region  填充列值
                            colName = colList[j];
                            value = dataTable.Rows[i][colName];
                            insertParValues.Append("?" + colName);
                            insertParValues.Append(colList.Count == j + 1 ? "" : ",");
                            mySqlcmd.Parameters.Add(new MySqlParameter("?" + colName, value));

                            if (value == null
                                ||
                                string.IsNullOrEmpty(value.ToString())
                                )
                            {
                                insertValues.Append("null");
                            }
                            else
                            {
                                insertValues.AppendFormat("'{0}'", value.ToString().Replace('\\', ' '));
                                //insertValues.AppendFormat("'{0}'",value.ToString().Replace('\\',' ').Replace('"','\"'));
                            }

                            insertValues.Append(colList.Count == j + 1 ? "" : ",");
                            #endregion
                        }

                        insertParValues.Append(" )");

                        insertValues.Append(" )");

                        #region 执行Insert
                        try
                        {
                            mySqlcmd.CommandText = insertSql.ToString() + insertParValues.ToString();
                            mySqlcmd.CommandType = CommandType.Text;
                            mySqlcmd.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            Console.WriteLine(mySqlcmd.CommandText);
                            Write(insertSql.ToString() + insertValues.ToString());
                        }
                        #endregion

                        #region 清空对象
                        insertParValues.Clear();
                        insertValues.Clear();
                        mySqlcmd.Parameters.Clear();
                        #endregion

                    }
                    #endregion

                    #region 单个线程执行导入结束
                    lock (curobj)
                    {
                        curIndex++;
                    }

                    endDate = DateTime.Now;
                    Console.WriteLine(string.Format("Table:{0}->BeginImport:{1}-EndImport:{2}-Cur/Total:{3}/{4}",
                                                     tableName, beginDate, endDate, curIndex, totalIndex)
                                      );
                    #endregion

                }
                #endregion

                #region 关闭数据库

                lock (curthreadobj)
                {
                    curthread++;
                }

                CloseConn(impDataP.oracleConn);
                CloseConn(impDataP.mySqlConn);

                #endregion
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Write(ex.Message);
            }
        }

        /// <summary>
        /// 打开Orcacle数据库连接
        /// </summary>
        /// <returns></returns>
        static OracleConnection OpenOracleConn()
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
        static MySqlConnection OpenMySqlConn()
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
        static void CloseConn(IDbConnection conn)
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

        /// <summary>
        /// 写入日志
        /// </summary>
        /// <param name="longStr"></param>
        /// <param name="textName"></param>
        static void Write(string longStr, string textName = "ErrorLog")
        {
            lock (obj)
            {
                StreamWriter sw = new StreamWriter(baseDir + "\\" + textName + ".txt", true);
                sw.WriteLine(longStr);
                sw.Close();//写入
            }
        }

        /// <summary>
        /// 检测线程完成度
        /// </summary>
        static void keepLive()
        {
            while (true)
            {
                Console.WriteLine(string.Format("CurIndex/TotalIndex:{0}/{1}",
                                                curIndex, totalIndex
                                                )
                                  );
                Console.WriteLine(string.Format("Curthread/ThreadCount:{0}/{1}",
                                                curthread, threadCount
                                                )
                                  );
                if (curthread == threadCount)
                {
                    endDate = DateTime.Now;
                }
                Console.WriteLine(string.Format("{0}-StartDate/EndDate:{1} / {2}",
                                               DateTime.Now, startDate, endDate
                                                )
                                  );
                Thread.Sleep(1000 * 60 * 1);
            }
        }

        /// <summary>
        /// 分类按数据量
        /// </summary>
        /// <param name="tableList"></param>
        static void FileTableByDataVolume(List<TableAttribute> tableList)
        {
            bool Isbeyond = false;//是否超出
            foreach (var item in tableList)
            {
                long minSize = 0;
                long maxSize = 0;
                do
                {
                    maxSize = minSize + SingleDatavolume;
                    Isbeyond = maxSize >= item.Count;
                    if (Isbeyond)
                    {
                        maxSize = item.Count;
                    }
                    ImportDataConfigList.Add(new ImportDataConfig(minSize + 1, maxSize + 1, item.TableName));

                    minSize = maxSize;
                }
                while (!Isbeyond);

            }
        }

        /// <summary>
        /// 获取SingleImportData
        /// </summary>
        /// <param name="min"></param>
        /// <returns></returns>
        static SingleImportData GetSingleImportData(ref int min)
        {
            SingleImportData singleImportData = new SingleImportData(ThreadDatavolume);
            for (; min < ImportDataConfigList.Count; )
            {
                ImportDataConfig importDataConfig = ImportDataConfigList[min];
                min++;
                if (singleImportData.SurplusCount > importDataConfig.Count)
                {
                    singleImportData.ImportDataList.Add(importDataConfig);
                    singleImportData.SurplusCount -= importDataConfig.Count;
                }
                else
                {
                    break;
                }

            }
            return singleImportData;
        }

    }

    /// <summary>
    /// 导入数据参数
    /// </summary>
    class ImportDataParameter
    {
        public ImportDataParameter(SingleImportData singleImportData, OracleConnection oracleConn, MySqlConnection mySqlConn)
        {
            this.SingleImportData = singleImportData;
            this.oracleConn = oracleConn;
            this.mySqlConn = mySqlConn;
        }
        public SingleImportData SingleImportData { get; set; }
        public OracleConnection oracleConn { get; set; }
        public MySqlConnection mySqlConn { get; set; }
    }

    /// <summary>
    /// 表属性
    /// </summary>
    class TableAttribute
    {
        public TableAttribute(string tableName, long count)
        {
            this.TableName = tableName;
            this.Count = count;
        }
        public string TableName { get; set; }

        public long Count { get; set; }
    }

    /// <summary>
    /// 线程处理数据量配置
    /// </summary>
    class SingleImportData
    {

        public SingleImportData(long threadDatavolume)
        {
            this.TotalCount = threadDatavolume;
            this.SurplusCount = threadDatavolume;
            this.ImportDataList = new List<ImportDataConfig>();
        }

        /// <summary>
        /// 可处理总数据量
        /// </summary>
        public long TotalCount { get; set; }
        /// <summary>
        /// 剩余处理数据量
        /// </summary>
        public long SurplusCount { get; set; }
        /// <summary>
        /// 数据配置集合
        /// </summary>
        public List<ImportDataConfig> ImportDataList { get; set; }

    }

    /// <summary>
    /// 数据配置
    /// </summary>
    class ImportDataConfig
    {
        public ImportDataConfig(long minSize, long maxSize, string tableName)
        {
            this.TableName = tableName;
            this.MinSize = minSize;
            this.MaxSize = maxSize;
            this.Count = this.MaxSize - this.MinSize;
        }
        public string TableName { get; set; }
        public long MinSize { get; set; }
        public long MaxSize { get; set; }
        public long Count { get; set; }

    }

}





