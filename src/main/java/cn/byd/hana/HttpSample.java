package cn.byd.hana;


import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.List;

/**
 * Created by huang.hai7 on 2021/6/25.
 */
public class HttpSample {


    public static Byd getData() {
        String url = "https://weixin33.bydauto.com.cn/home/Sincerely/shopCommentList";
        JsonObject param = new JsonObject();
        param.addProperty("token", "a2f17038-b252-479d-a448-9bd2a857da44");
        param.addProperty("startDate", "2020-12-01");
        param.addProperty("endDate", "2021-01-06");
        System.out.println("Param: " + param.toString());
        String response = HttpUtil.httpPostWithJSON(url, null);
//        System.out.println(response);

        Gson gson = new Gson();
        Byd byd = gson.fromJson(response, Byd.class);

        String code = byd.getCode();
        String msg = byd.getMsg();

        List<Data> list = byd.getData();
        return byd;
    }
}
