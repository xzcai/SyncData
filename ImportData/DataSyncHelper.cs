using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.IO;
using Newtonsoft.Json;


namespace ImportData
{
    public static class DataSyncHelper
    {

       



        /// <summary>
        /// Json序列化,用于socket发送
        /// </summary>
        public static string ToJsJson123(this object entity)
        {
            return JsonConvert.SerializeObject(entity);
        }



        public static T FromJsonTo<T>(this string jsonCmd)
        {
            T jsonObj = (T)JsonConvert.DeserializeObject<T>(jsonCmd);
            return jsonObj;
        }

        public static List<T> FromJsonToList<T>(this string jsonCmd)
        {
            List<T> jsonObj = (List<T>)JsonConvert.DeserializeObject<List<T>>(jsonCmd);  

            return jsonObj;
        } 

    }
}
