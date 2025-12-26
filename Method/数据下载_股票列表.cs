using DllBulider;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Method
{
    public class 数据下载_股票列表
    {
        public static void _下载()
        {
            string api = Method.api.mydata_url_股票列表 + Method.token.mydata_license01;
            System.Diagnostics.Debug.WriteLine($"当前正在调用的API地址为: {api}");
            string JsonReturn = 处理HttpClient._Get(api, queryString: null, timeoutSeconds: 30);
            System.Diagnostics.Debug.WriteLine($"当前获取到的Json为: {JsonReturn}");
            if (string.IsNullOrWhiteSpace(JsonReturn))
            {
                MessageBox.Show("API 返回为空。", "提示", MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }
            // 解析JSON
            JArray jsonArray = JArray.Parse(JsonReturn);
            // 转换为DataTable
            DataTable dt = new DataTable();
            dt.Columns.Add("代码", typeof(string));
            dt.Columns.Add("名称", typeof(string));
            dt.Columns.Add("交易所", typeof(string));
            foreach (JObject item in jsonArray)
            {
                string dm = item["dm"]?.ToString().Trim();
                //去除.及后面的字符
                dm = dm.Split('.')[0];
                string mc = item["mc"]?.ToString().Trim();
                string jys = item["jys"]?.ToString().Trim();
                dt.Rows.Add(dm, mc, jys);
            }
            _批量写入(dt);
        }

        /// <summary>
        /// 使用批量 INSERT 语句写入 (备用方案，5000行约5-10秒)
        /// </summary>
        static void _批量写入(DataTable dt)
        {
            //先删
            DBProvide_Sql._Exec_Text(DBLinksPerson.Person, "TRUNCATE TABLE dbo.基础数据_股票列表");
            //批量写入
            int batchSize = 1000; // 每批500条
            List<string> batches = new List<string>();

            for (int i = 0; i < dt.Rows.Count; i += batchSize)
            {
                StringBuilder sql = new StringBuilder();
                sql.Append("INSERT INTO dbo.基础数据_股票列表(股票代码,股票名称,交易所,更新时间) VALUES ");

                List<string> values = new List<string>();
                for (int j = i; j < Math.Min(i + batchSize, dt.Rows.Count); j++)
                {
                    DataRow row = dt.Rows[j];
                    string 股票代码 = row["代码"].ToString().Trim().Replace("'", "''");
                    string 股票名称 = row["名称"].ToString().Trim().Replace("'", "''");
                    string 交易所 = row["交易所"].ToString().Trim().Replace("'", "''");
                    values.Add($"('{股票代码}','{股票名称}','{交易所}',GETDATE())");
                }

                sql.Append(string.Join(",", values));
                Console.WriteLine("股票列表下载：" + sql.ToString().Trim());
                DBProvide_Sql._Exec_Text(DBLinksPerson.Person, sql.ToString().Trim());
            }
            _数据清理();
        }

        public static void _数据清理()
        {
            //移除ST的股票
            string deleteSql = "DELETE FROM dbo.基础数据_股票列表 WHERE 股票名称 LIKE '%S%' OR 股票名称 LIKE '%T%'";
            DBProvide_Sql._Exec_Text(DBLinksPerson.Person, deleteSql);
            //更新数据日期
            DBProvide_Sql._Exec_Proc(DBLinksPerson.Person, "_Proc_基础数据_股票列表_更新历史数据最新日期");
        }
    }
}
