using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Method
{
    public class token
    {
        public static string tushare = "aadcd6f7830d81511ceb66928766afcf4a810e35b97382197550b2fe";
        public static string mydata_license01 = "14E72731-A456-4968-A940-7C3C3E626670";//个人黄金版，每日10000次
        public static string mydata_license02 = "4687D9C1-8367-4A6D-AC6C-291EE371229B";//个人免费版，每日50次
    }

    public class api
    {
        public static string mydata_url_股票列表 = "https://api.mairuiapi.com/hslt/list/";
        public static string mydata_url_历史数据 = "https://api.mairuiapi.com/hsstock/history/";
        public static string mydata_url_当日数据 = "https://api.mairuiapi.com/hsrl/ssjy_more/";
        //public static string mydata_url_历史数据 = "https://api.mairuiapi.com/hsstock/history/000001.SZ/d/n/4687D9C1-8367-4A6D-AC6C-291EE371229B";
    }
}
