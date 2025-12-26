using DllBulider;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Method
{
    public class 数据下载_当日K线
    {
        public static void _下载()
        {
            // 创建并显示进度窗体
            AppoTrade.基础数据.历史数据下载进度 progressForm = new AppoTrade.基础数据.历史数据下载进度();

            // 异步执行下载任务
            Task.Run(() =>
            {
                try
                {
                    _执行当日数据下载(progressForm);
                }
                catch (Exception ex)
                {
                    // 设置错误
                    progressForm.SetError(ex.Message);
                    System.Diagnostics.Debug.WriteLine($"当日数据下载失败: {ex.Message}");
                }
            });

            // 显示进度窗体（阻塞UI直到关闭）
            progressForm.ShowDialog();
        }

        private static void _执行当日数据下载(AppoTrade.基础数据.历史数据下载进度 progressForm)
        {
            //获取股票列表
            string sql_列表 = "SELECT 股票代码 FROM dbo.基础数据_股票列表";
            DataTable dt_列表 = DBProvide_Sql._Fill_Text(DBLinksPerson.Person, sql_列表);
            //每20个格式化后股票代码为一组，中间用,号分割
            List<string> codeGroups = new List<string>();
            for (int i = 0; i < dt_列表.Rows.Count; i += 20)
            {
                var group = dt_列表.AsEnumerable().Skip(i).Take(20)
                    .Select(row => row.Field<string>("股票代码"));
                codeGroups.Add(string.Join(",", group));
            }

            // 转换为DataTable
            DataTable dt = new DataTable();
            dt.Columns.Add("股票代码", typeof(string));
            dt.Columns.Add("日期", typeof(string));
            dt.Columns.Add("开盘价", typeof(decimal));
            dt.Columns.Add("最高价", typeof(decimal));
            dt.Columns.Add("最低价", typeof(decimal));
            dt.Columns.Add("收盘价", typeof(decimal));
            dt.Columns.Add("成交量", typeof(long));
            dt.Columns.Add("成交额", typeof(decimal));
            dt.Columns.Add("前收盘价", typeof(decimal));
            dt.Columns.Add("是否停牌", typeof(int));

            int totalGroups = codeGroups.Count;
            int currentGroupIndex = 0;

            foreach (var codeGroup in codeGroups)
            {
                // 检查是否取消
                if (progressForm.IsCancelled)
                {
                    progressForm.SetError("用户取消操作");
                    return;
                }

                currentGroupIndex++;
                bool isSuccess = false;
                int dataCount = 0;
                string errorMessage = "";
                string errorType = "";
                int finalRetryCount = 0;

                try
                {
                    string api = Method.api.mydata_url_当日数据 + token.mydata_license01 + "?stock_codes=" + codeGroup;
                    System.Diagnostics.Debug.WriteLine($"当前正在调用的API地址为: {api}");

                    // 重试机制：最多尝试10次
                    string JsonReturn = null;
                    int maxRetries = 10;
                    int attemptCount = 0;
                    JArray jsonArray = null;

                    while (attemptCount < maxRetries)
                    {
                        attemptCount++;

                        try
                        {
                            // 显示当前尝试状态
                            if (attemptCount > 1)
                            {
                                int retryNum = attemptCount - 1;
                                finalRetryCount = retryNum;
                                System.Diagnostics.Debug.WriteLine($"正在重试下载股票组 {codeGroup}，第 {retryNum} 次重试（第{attemptCount}次尝试）");
                                progressForm.UpdateProgress(currentGroupIndex, totalGroups, codeGroup, dataCount, false, retryNum);
                            }

                            // 优化：根据重试次数动态调整超时时间
                            int timeoutSeconds = attemptCount <= 3 ? 30 : (attemptCount <= 6 ? 45 : 60);

                            JsonReturn = 处理HttpClient._Get(api, queryString: null, timeoutSeconds);
                            System.Diagnostics.Debug.WriteLine($"当前获取到的Json为: {JsonReturn}");

                            // 检查是否返回错误
                            if (!string.IsNullOrWhiteSpace(JsonReturn))
                            {
                                string trimmedJson = JsonReturn.Trim();
                                if (trimmedJson.StartsWith("{"))
                                {
                                    try
                                    {
                                        JObject errorObj = JObject.Parse(JsonReturn);
                                        if (errorObj["error"] != null)
                                        {
                                            string errorMsg = errorObj["error"].ToString().Trim();
                                            if (errorMsg.Contains("数据不存在") || errorMsg.Contains("data does not exist"))
                                            {
                                                // 数据不存在，记录错误但继续
                                                System.Diagnostics.Debug.WriteLine($"股票组 {codeGroup} 数据不存在");
                                                _记录下载失败(codeGroup, "数据不存在", "API返回数据不存在");
                                                progressForm.UpdateProgress(currentGroupIndex, totalGroups, codeGroup, 0, false, 0);
                                                break;
                                            }
                                        }
                                    }
                                    catch
                                    {
                                        // 不是标准错误对象，继续正常处理
                                    }
                                }
                            }

                            if (string.IsNullOrWhiteSpace(JsonReturn))
                            {
                                throw new Exception("API返回为空");
                            }

                            // 尝试解析JSON
                            jsonArray = JArray.Parse(JsonReturn);

                            // 如果成功解析且有数据，跳出重试循环
                            if (jsonArray != null && jsonArray.Count > 0)
                            {
                                break;
                            }
                            else
                            {
                                throw new Exception("JSON数组为空");
                            }
                        }
                        catch (Exception retryEx)
                        {
                            System.Diagnostics.Debug.WriteLine($"第{attemptCount}次尝试失败: {retryEx.Message}");

                            if (attemptCount < maxRetries)
                            {
                                // 指数退避策略
                                int baseDelay = 1000;
                                int maxDelay = 15000;
                                int sleepTime = Math.Min((int)(baseDelay * Math.Pow(2, attemptCount - 1)), maxDelay);
                                System.Diagnostics.Debug.WriteLine($"等待 {sleepTime / 1000.0:F1} 秒后重试...");
                                System.Threading.Thread.Sleep(sleepTime);
                            }
                            else
                            {
                                errorType = retryEx.GetType().Name;
                                errorMessage = retryEx.Message;
                            }
                        }
                    }

                    // 如果所有重试都失败
                    if (jsonArray == null || jsonArray.Count == 0)
                    {
                        int totalRetries = attemptCount - 1;
                        finalRetryCount = totalRetries;

                        if (string.IsNullOrEmpty(errorType))
                        {
                            errorType = "API返回为空或数据为空";
                        }
                        if (string.IsNullOrEmpty(errorMessage))
                        {
                            errorMessage = $"已尝试 {attemptCount} 次（重试 {totalRetries} 次），仍无法获取有效数据";
                        }
                        else
                        {
                            errorMessage = $"已尝试 {attemptCount} 次（重试 {totalRetries} 次），最后错误: {errorMessage}";
                        }

                        System.Diagnostics.Debug.WriteLine($"下载股票组 {codeGroup} 失败，{errorMessage}");

                        // 记录失败
                        _记录下载失败(codeGroup, errorType, errorMessage);

                        progressForm.UpdateProgress(currentGroupIndex, totalGroups, codeGroup, 0, false, totalRetries);
                        continue;
                    }

                    // 成功获取数据
                    dataCount = jsonArray.Count;
                    finalRetryCount = attemptCount - 1;

                    foreach (JObject item in jsonArray)
                    {
                        string 股票代码 = item["dm"].ToString().Trim();
                        decimal 开盘价 = Convert.ToDecimal(item["o"]);
                        decimal 最高价 = Convert.ToDecimal(item["h"]);
                        decimal 最低价 = Convert.ToDecimal(item["l"]);
                        decimal 收盘价 = Convert.ToDecimal(item["p"]);
                        long 成交量 = Convert.ToInt64(item["v"]);
                        decimal 成交额 = Convert.ToDecimal(item["cje"]);
                        DateTime 日期 = Convert.ToDateTime(item["t"]).Date;
                        decimal 前收盘价 = Convert.ToDecimal(item["yc"]);
                        int 是否停牌 = (开盘价 == 0 && 最高价 == 0 && 最低价 == 0 && 收盘价 == 0) ? 1 : 0;
                        dt.Rows.Add(股票代码, 日期, 开盘价, 最高价, 最低价, 收盘价, 成交量, 成交额, 前收盘价, 是否停牌);
                    }

                    isSuccess = true;

                    if (finalRetryCount > 0)
                    {
                        System.Diagnostics.Debug.WriteLine($"✓ 股票组 {codeGroup} 经过 {finalRetryCount} 次重试后成功下载，数据条数: {dataCount}");
                        progressForm.UpdateRetrySuccess(currentGroupIndex, totalGroups, codeGroup, dataCount, finalRetryCount);
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine($"✓ 股票组 {codeGroup} 下载成功，数据条数: {dataCount}");
                        progressForm.UpdateProgress(currentGroupIndex, totalGroups, codeGroup, dataCount, true, 0);
                    }
                }
                catch (Exception ex)
                {
                    errorType = ex.GetType().Name;
                    errorMessage = $"数据处理失败: {ex.Message}";
                    System.Diagnostics.Debug.WriteLine($"下载股票组 {codeGroup} 失败: {ex.Message}");

                    _记录下载失败(codeGroup, errorType, errorMessage);

                    isSuccess = false;
                    progressForm.UpdateProgress(currentGroupIndex, totalGroups, codeGroup, 0, false, 0);
                }
            }

            // 获取日期并处理数据
            if (dt.Rows.Count > 0)
            {
                string 当前日期 = Convert.ToDateTime(dt.Rows[0]["日期"].ToString().Trim()).ToString("yyyy-MM-dd");
                //删除当日旧数据，写入当日新数据
                string sql_del = $"DELETE dbo.基础数据_历史数据 WHERE  CONVERT(VARCHAR(10),日期,23)='{当前日期}'";
                DBProvide_Sql._Exec_Text(DBLinksPerson.Person, sql_del);
                _当日数据_批量写入(dt);
            }

            // 完成
            progressForm.SetCompleted();
            System.Diagnostics.Debug.WriteLine("当日数据下载完成");
        }

        /// <summary>
        /// 记录下载失败的股票组到数据库
        /// </summary>
        private static void _记录下载失败(string 股票组, string 错误类型, string 错误信息)
        {
            try
            {
                string code = 股票组.Replace("'", "''");
                string type = 错误类型.Replace("'", "''");
                string message = 错误信息.Replace("'", "''");

                if (message.Length > 500)
                {
                    message = message.Substring(0, 497) + "...";
                }

                string sql = $@"INSERT INTO dbo.基础数据_下载失败记录(股票代码, 错误类型, 错误信息, 失败时间) 
                               VALUES ('{code}', '{type}', '{message}', GETDATE())";

                DBProvide_Sql._Exec_Text(DBLinksPerson.Person, sql);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"记录下载失败信息时出错: {ex.Message}");
            }
        }

        /// <summary>
        /// 批量写入历史数据
        /// </summary>
        static void _当日数据_批量写入(DataTable dt)
        {
            if (dt.Rows.Count == 0)
            {
                return;
            }
            // 批量写入
            int batchSize = 1000;

            for (int i = 0; i < dt.Rows.Count; i += batchSize)
            {
                StringBuilder sql = new StringBuilder();
                sql.Append("INSERT INTO dbo.基础数据_历史数据(股票代码,日期,开盘价,最高价,最低价,收盘价,成交量,成交额,前收盘价,是否停牌,更新时间) VALUES ");

                List<string> values = new List<string>();
                for (int j = i; j < Math.Min(i + batchSize, dt.Rows.Count); j++)
                {
                    DataRow row = dt.Rows[j];
                    string code = row["股票代码"].ToString().Trim().Replace("'", "''");
                    string date = row["日期"].ToString().Trim().Replace("'", "''");
                    string kpj = row["开盘价"].ToString().Trim();
                    string zgj = row["最高价"].ToString().Trim();
                    string zdj = row["最低价"].ToString().Trim();
                    string spj = row["收盘价"].ToString().Trim();
                    string cjl = row["成交量"].ToString().Trim();
                    string cje = row["成交额"].ToString().Trim();
                    string qspj = row["前收盘价"].ToString().Trim();
                    string sftp = row["是否停牌"].ToString().Trim();

                    values.Add($"('{code}','{date}',{kpj},{zgj},{zdj},{spj},{cjl},{cje},{qspj},{sftp},GETDATE())");
                }

                sql.Append(string.Join(",", values));
                Debug.WriteLine($"正在执行的SQL语句为: {sql.ToString().Trim()}");
                DBProvide_Sql._Exec_Text(DBLinksPerson.Person, sql.ToString().Trim());
            }
        }
    }
}