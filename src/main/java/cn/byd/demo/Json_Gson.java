package cn.byd.demo;


import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.Date;

public class Json_Gson {
    public static void main(String[] args) {
        // gson无法正常将时间戳转化成date
        // 使用JSON内存树注册Date类型
        final Gson gson = new GsonBuilder()
                .registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
                    @Override
                    public Date deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
                        return new Date(jsonElement.getAsJsonPrimitive().getAsLong());
                    }
                }).create();

        String jsonString = "{'id':1001, 'firstName':'Lokesh', 'lastName':'Gupta', 'email':'howtodoinjava@gmail.com'}";

        Gson s = new Gson();

        Employee empObject = gson.fromJson(jsonString, Employee.class);

        System.out.println(empObject);

    }
}
