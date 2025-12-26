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
    public class 数据下载_历史K线
    {
        // 缓存最新日期，避免重复请求
        private static string _缓存最新日期 = null;
        private static DateTime _缓存最新日期时间 = DateTime.MinValue;
        private static readonly TimeSpan _缓存有效期 = TimeSpan.FromMinutes(5);

        public static void _下载()
        {
            // 清除缓存，确保获取最新数据
            _缓存最新日期 = null;

            // 创建并显示进度窗体
            AppoTrade.基础数据.历史数据下载进度 progressForm = new AppoTrade.基础数据.历史数据下载进度();

            // 异步执行下载任务
            Task.Run(() =>
            {
                try
                {
                    _执行历史数据下载(progressForm);
                }
                catch (Exception ex)
                {
                    progressForm.SetError(ex.Message);
                }
            });

            // 显示进度窗体（阻塞UI直到关闭）
            progressForm.ShowDialog();
        }

        private static void _执行历史数据下载(AppoTrade.基础数据.历史数据下载进度 progressForm)
        {
            //更新股票列表中数据最新日期
            DBProvide_Sql._Exec_Proc(DBLinksPerson.Person, "_Proc_基础数据_股票列表_更新历史数据最新日期");

            string 最新日期 = _获取数据最新日期();
            //获取股票列表中的所有股票代码
            DataTable dt股票列表 = DBProvide_Sql._Fill_Text(DBLinksPerson.Person, $"SELECT 股票代码+'.'+交易所 FROM dbo.基础数据_股票列表 WHERE ISNULL(数据最新日期,'1990-01-01')<'{最新日期}'");

            int totalCount = dt股票列表.Rows.Count;
            int currentIndex = 0;

            foreach (DataRow row in dt股票列表.Rows)
            {
                // 检查是否取消
                if (progressForm.IsCancelled)
                {
                    progressForm.SetError("用户取消操作");
                    return;
                }

                currentIndex++;
                string 股票代码 = row["股票代码"].ToString().Trim();
                bool isSuccess = false;
                int dataCount = 0;
                string errorMessage = "";
                string errorType = "";
                int finalRetryCount = 0; // 最终的重试次数
                bool shouldSkip = false; // 标记是否应该跳过后续处理（比如数据不存在）

                try
                {
                    string 级别 = "/d/f/"; // 日线级别
                    string api = Method.api.mydata_url_历史数据 + 股票代码 + 级别 + Method.token.mydata_license01;
                    System.Diagnostics.Debug.WriteLine($"当前正在调用的API地址为: {api}");

                    // 重试机制：最多尝试10次
                    string JsonReturn = null;
                    int maxRetries = 10;
                    int attemptCount = 0; // 尝试次数
                    JArray jsonArray = null;

                    while (attemptCount < maxRetries)
                    {
                        attemptCount++; // 先增加尝试次数

                        try
                        {
                            // 显示当前尝试状态
                            if (attemptCount > 1)
                            {
                                int retryNum = attemptCount - 1; // 重试次数 = 尝试次数 - 1
                                finalRetryCount = retryNum;
                                System.Diagnostics.Debug.WriteLine($"正在重试下载股票 {股票代码}，第 {retryNum} 次重试（第{attemptCount}次尝试）");
                                Console.WriteLine($"正在重试下载股票 {股票代码}，第 {retryNum} 次重试（第{attemptCount}次尝试）");
                                progressForm.UpdateProgress(currentIndex, totalCount, 股票代码, dataCount, false, retryNum);
                            }

                            // 优化：根据重试次数动态调整超时时间
                            // 第1-3次尝试：30秒超时（快速失败）
                            // 第4-6次尝试：45秒超时
                            // 第7次及以后：60秒超时
                            int timeoutSeconds = attemptCount <= 3 ? 30 : (attemptCount <= 6 ? 45 : 60);

                            JsonReturn = 处理HttpClient._Get(api, queryString: null, timeoutSeconds: timeoutSeconds);
                            System.Diagnostics.Debug.WriteLine($"当前获取到的Json为: {JsonReturn}");

                            // 检查是否返回"数据不存在"错误
                            if (!string.IsNullOrWhiteSpace(JsonReturn))
                            {
                                // 先检查是否以 { 开头（可能是错误对象）
                                string trimmedJson = JsonReturn.Trim();
                                if (trimmedJson.StartsWith("{"))
                                {
                                    // 尝试解析为错误对象
                                    try
                                    {
                                        JObject errorObj = JObject.Parse(JsonReturn);
                                        if (errorObj["error"] != null)
                                        {
                                            string errorMsg = errorObj["error"].ToString().Trim();
                                            if (errorMsg.Contains("数据不存在") || errorMsg.Contains("data does not exist"))
                                            {
                                                // 数据不存在，删除股票列表中的该股票
                                                System.Diagnostics.Debug.WriteLine($"股票 {股票代码} 数据不存在，从股票列表中删除");
                                                Console.WriteLine($"股票 {股票代码} 数据不存在，从股票列表中删除");

                                                string deleteSql = $"DELETE FROM dbo.基础数据_股票列表 WHERE 股票代码='{股票代码.Replace("'", "''")}'";
                                                DBProvide_Sql._Exec_Text(DBLinksPerson.Person, deleteSql);

                                                // 记录到失败记录表
                                                _记录下载失败(股票代码, "数据不存在", "API返回数据不存在，已从股票列表中删除");

                                                // 更新进度显示
                                                progressForm.UpdateProgress(currentIndex, totalCount, 股票代码, 0, false, 0);

                                                // 标记为需要跳过，并跳出重试循环
                                                shouldSkip = true;
                                                break;
                                            }
                                        }
                                    }
                                    catch
                                    {
                                        // 不是标准错误对象格式，继续正常处理
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
                            // 记录每次尝试的错误
                            System.Diagnostics.Debug.WriteLine($"第{attemptCount}次尝试失败: {retryEx.Message}");
                            Console.WriteLine($"第{attemptCount}次尝试失败: {retryEx.Message}");

                            // 如果还没到最大次数，等待后继续
                            if (attemptCount < maxRetries)
                            {
                                // 优化：使用指数退避策略（1秒、2秒、4秒、8秒...最多15秒）
                                // 而不是线性增长（3秒、6秒、9秒...）
                                int baseDelay = 1000; // 1秒基础延迟
                                int maxDelay = 15000; // 最大15秒
                                int sleepTime = Math.Min((int)(baseDelay * Math.Pow(2, attemptCount - 1)), maxDelay);

                                System.Diagnostics.Debug.WriteLine($"等待 {sleepTime / 1000.0:F1} 秒后重试...");
                                System.Threading.Thread.Sleep(sleepTime);
                            }
                            else
                            {
                                // 已达到最大重试次数，记录最后的错误
                                errorType = retryEx.GetType().Name;
                                errorMessage = retryEx.Message;
                            }
                        }
                    }

                    // 如果标记为跳过（数据不存在），直接继续下一个股票
                    if (shouldSkip)
                    {
                        continue;
                    }

                    // 如果所有重试都失败
                    if (jsonArray == null || jsonArray.Count == 0)
                    {
                        int totalRetries = attemptCount - 1; // 总重试次数 = 总尝试次数 - 1
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

                        System.Diagnostics.Debug.WriteLine($"下载股票 {股票代码} 失败，{errorMessage}");
                        Console.WriteLine($"下载股票 {股票代码} 失败，{errorMessage}");

                        // 记录失败到数据库
                        _记录下载失败(股票代码, errorType, errorMessage);

                        progressForm.UpdateProgress(currentIndex, totalCount, 股票代码, 0, false, totalRetries);
                        continue;
                    }

                    // 成功获取数据，开始处理
                    dataCount = jsonArray.Count;
                    finalRetryCount = attemptCount - 1; // 记录实际重试次数

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

                    foreach (JObject item in jsonArray)
                    {
                        string rq = item["t"]?.ToString().Trim(); // 日期
                        decimal kpj = item["o"] != null ? Convert.ToDecimal(item["o"].ToString().Trim()) : 0; // 开盘价
                        decimal zgj = item["h"] != null ? Convert.ToDecimal(item["h"].ToString().Trim()) : 0; // 最高价
                        decimal zdj = item["l"] != null ? Convert.ToDecimal(item["l"].ToString().Trim()) : 0; // 最低价
                        decimal spj = item["c"] != null ? Convert.ToDecimal(item["c"].ToString().Trim()) : 0; // 收盘价
                        long cjl = item["v"] != null ? Convert.ToInt64(item["v"].ToString().Trim()) : 0; // 成交量
                        decimal cje = item["a"] != null ? Convert.ToDecimal(item["a"].ToString().Trim()) : 0; // 成交额
                        decimal qspj = item["pc"] != null ? Convert.ToDecimal(item["pc"].ToString().Trim()) : 0; // 前收盘价
                        decimal sftp = item["sf"] != null ? Convert.ToDecimal(item["sf"].ToString().Trim()) : 0; // 是否停牌
                        dt.Rows.Add(股票代码, rq, kpj, zgj, zdj, spj, cjl, cje, qspj, sftp);
                    }

                    //删除旧数据，批量写入新数据
                    string sql_del = $"DELETE dbo.基础数据_历史数据 WHERE 股票代码='{股票代码}'";
                    DBProvide_Sql._Exec_Text(DBLinksPerson.Person, sql_del);
                    _历史数据_批量写入(dt);

                    isSuccess = true;

                    // 判断是否经过重试才成功
                    if (finalRetryCount > 0)
                    {
                        // 重试后成功
                        System.Diagnostics.Debug.WriteLine($"✓ 股票 {股票代码} 经过 {finalRetryCount} 次重试后成功下载，数据条数: {dataCount}");
                        //Console.WriteLine($"✓ 股票 {股票代码} 经过 {finalRetryCount} 次重试后成功下载，数据条数: {dataCount}");
                        progressForm.UpdateRetrySuccess(currentIndex, totalCount, 股票代码, dataCount, finalRetryCount);
                    }
                    else
                    {
                        // 第一次就成功
                        System.Diagnostics.Debug.WriteLine($"✓ 股票 {股票代码} 下载成功，数据条数: {dataCount}");
                        //Console.WriteLine($"✓ 股票 {股票代码} 下载成功，数据条数: {dataCount}");
                        progressForm.UpdateProgress(currentIndex, totalCount, 股票代码, dataCount, true, 0);
                    }
                }
                catch (Exception ex)
                {
                    // 这里只处理数据写入过程中的错误（非重试范围内的错误）
                    errorType = ex.GetType().Name;
                    errorMessage = $"数据处理/写入失败: {ex.Message}";
                    System.Diagnostics.Debug.WriteLine($"下载股票 {股票代码} 失败: {ex.Message}");
                    Console.WriteLine($"下载股票 {股票代码} 失败: {ex.Message}");

                    // 记录失败到数据库
                    _记录下载失败(股票代码, errorType, errorMessage);

                    isSuccess = false;
                    progressForm.UpdateProgress(currentIndex, totalCount, 股票代码, 0, false, 0);
                }

            }

            // 完成
            progressForm.SetCompleted();
            //更新股票列表中数据最新日期
            DBProvide_Sql._Exec_Proc(DBLinksPerson.Person, "_Proc_基础数据_股票列表_更新历史数据最新日期");
        }

        /// <summary>
        /// 记录下载失败的股票到数据库
        /// </summary>
        /// <param name="股票代码">股票代码</param>
        /// <param name="错误类型">错误类型</param>
        /// <param name="错误信息">错误详细信息</param>
        private static void _记录下载失败(string 股票代码, string 错误类型, string 错误信息)
        {
            try
            {
                // 转义单引号防止SQL注入
                string code = 股票代码.Replace("'", "''");
                string type = 错误类型.Replace("'", "''");
                string message = 错误信息.Replace("'", "''");

                // 限制错误信息长度不超过500字符
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
                // 记录失败日志，但不影响主流程
                System.Diagnostics.Debug.WriteLine($"记录下载失败信息时出错: {ex.Message}");
            }
        }

        /// <summary>
        /// 批量写入历史数据
        /// </summary>
        static void _历史数据_批量写入(DataTable dt)
        {
            if (dt.Rows.Count == 0)
            {
                return;
            }

            // 获取股票代码用于删除该股票的旧数据
            string 股票代码 = dt.Rows[0]["股票代码"].ToString().Trim().Replace("'", "''");

            // 先删除该股票的历史数据
            string deleteSql = $"DELETE FROM dbo.基础数据_历史数据 WHERE 股票代码='{股票代码}'";
            DBProvide_Sql._Exec_Text(DBLinksPerson.Person, deleteSql);

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
                DBProvide_Sql._Exec_Text(DBLinksPerson.Person, sql.ToString().Trim());
            }
        }



        private static string _获取数据最新日期()
        {
            // 检查缓存是否有效
            if (_缓存最新日期 != null &&
                DateTime.Now - _缓存最新日期时间 < _缓存有效期)
            {
                System.Diagnostics.Debug.WriteLine($"使用缓存的最新日期: {_缓存最新日期}");
                return _缓存最新日期;
            }

            string 日期 = "";
            //获取股票列表中的所有股票代码
            string 股票代码 = DBProvide_Sql._Scalar_Text(DBLinksPerson.Person, "SELECT top 1 股票代码+'.'+交易所 FROM dbo.基础数据_股票列表");

            string 级别 = "/d/f/"; // 日线级别
            string api = Method.api.mydata_url_历史数据 + 股票代码 + 级别 + Method.token.mydata_license01;

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
                        System.Diagnostics.Debug.WriteLine($"正在重试获取数据最新日期，第 {retryNum} 次重试（第{attemptCount}次尝试）");
                        Console.WriteLine($"正在重试获取数据最新日期，第 {retryNum} 次重试（第{attemptCount}次尝试）");
                    }

                    // 优化：使用较短的超时时间，获取日期通常很快
                    int timeoutSeconds = attemptCount <= 3 ? 20 : (attemptCount <= 6 ? 30 : 45);

                    JsonReturn = 处理HttpClient._Get(api, queryString: null, timeoutSeconds: timeoutSeconds);
                    System.Diagnostics.Debug.WriteLine($"当前获取到的Json为: {JsonReturn}");

                    if (string.IsNullOrWhiteSpace(JsonReturn))
                    {
                        throw new Exception("API返回为空");
                    }

                    // 尝试解析JSON（添加错误检查）
                    string trimmedJson = JsonReturn.Trim();

                    // 检查是否是错误响应（以 { 开头可能是错误对象）
                    if (trimmedJson.StartsWith("{"))
                    {
                        try
                        {
                            JObject errorObj = JObject.Parse(JsonReturn);
                            if (errorObj["error"] != null)
                            {
                                throw new Exception($"API返回错误: {errorObj["error"]}");
                            }
                        }
                        catch (Newtonsoft.Json.JsonReaderException)
                        {
                            // JSON格式错误，继续抛出原始错误
                            throw new Exception("API返回格式错误");
                        }
                    }

                    jsonArray = JArray.Parse(JsonReturn);

                    // 如果成功解析且有数据，跳出重试循环
                    if (jsonArray != null && jsonArray.Count > 0)
                    {
                        System.Diagnostics.Debug.WriteLine($"✓ 成功获取数据最新日期，尝试次数: {attemptCount}");
                        break;
                    }
                    else
                    {
                        throw new Exception("JSON数组为空");
                    }
                }
                catch (Exception retryEx)
                {
                    // 记录每次尝试的错误
                    System.Diagnostics.Debug.WriteLine($"第{attemptCount}次尝试失败: {retryEx.Message}");

                    // 如果还没到最大次数，等待后继续
                    if (attemptCount < maxRetries)
                    {
                        // 优化：使用指数退避策略
                        int baseDelay = 1000;
                        int maxDelay = 10000; // 获取日期的重试延迟可以更短
                        int sleepTime = Math.Min((int)(baseDelay * Math.Pow(2, attemptCount - 1)), maxDelay);

                        System.Diagnostics.Debug.WriteLine($"等待 {sleepTime / 1000.0:F1} 秒后重试...");
                        System.Threading.Thread.Sleep(sleepTime);
                    }
                }
            }

            // 如果所有重试都失败
            if (jsonArray == null || jsonArray.Count == 0)
            {
                string errorMsg = $"获取数据最新日期失败，已尝试 {attemptCount} 次（重试 {attemptCount - 1} 次）";
                System.Diagnostics.Debug.WriteLine(errorMsg);
                Console.WriteLine(errorMsg);
                MessageBox.Show(errorMsg, "提示", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return 日期;
            }

            // 成功获取，提取最后一条记录的日期（最新日期）
            foreach (JObject item in jsonArray)
            {
                日期 = item["t"]?.ToString().Trim(); // 日期
            }

            // 缓存结果
            if (!string.IsNullOrEmpty(日期))
            {
                _缓存最新日期 = 日期;
                _缓存最新日期时间 = DateTime.Now;
                System.Diagnostics.Debug.WriteLine($"缓存最新日期: {日期}");
            }

            return 日期;
        }
    }
}
