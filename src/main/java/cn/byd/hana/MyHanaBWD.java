package cn.byd.hana;


import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.sql.*;
import java.util.List;


/**
 * Created by su.gaoming on 2021/8/30.
 */
public class MyHanaBWD {
    private static final String DRIVER = "com.sap.db.jdbc.Driver";

    private static final String USER = "WUMIAOMIAO";

    private static final String PWD = "Byd;2106";

    private static final String URL = "jdbc:sap://192.168.100.204:30015?reconnect=true";

//    private static final String SQL = "";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        Byd byd = HttpSample.getData();
        List<Data> list = byd.getData();

        /*Class.forName(DRIVER);
        System.out.println("Start Connection.");
        Connection conn = DriverManager.getConnection(URL, USER, PWD);
        Statement sql = conn.createStatement();
        sql.executeUpdate("delete from \"SAPABAP1\".\"/BIC/AZEZJT3572\"");*/

        if (list != null) {
            Class.forName(DRIVER);
            System.out.println("Start Connection.");
            Connection conn = DriverManager.getConnection(URL, USER, PWD);
            Statement sql = conn.createStatement();
            for (int i = 0; i < list.size(); i++) {
                sql.executeUpdate("insert into  \"WUMIAOMIAO\".\"ZT_SHSJ_001\" (ID,GROUP_SN,SN,JOIN_SHOP_NAME,SERVICE_SHOP_NO,GOODS_TYPE,GOODS_NAME,TC_NAME,NICKNAME,MOBILE,ORDER_STATUS,CONFIRM_TIME,CREATE_TIME,CONTENT,REPLY_CONTENT,ZP_CONTENT,SKILL_SCORE,ATTITUDE_SCORE,ENV_SCORE,SYNTHESIZE_SCORE,UPDATE_TIME,IS_ZP,IS_REPLY) values('" +
                        list.get(i).getId() + "','" + list.get(i).getJoin_shop_name() + "','" + list.get(i).getService_shop_no() + "','" + list.get(i).getGroup_sn() + "','" +
                        list.get(i).getSn() + "','" + list.get(i).getGoods_type() + "','" + list.get(i).getGoods_name() + "','" + list.get(i).getTc_name() + "','" + list.get(i).getNickname() + "','" +
                        list.get(i).getMobile() + "','" + list.get(i).getOrder_status() + "','" + list.get(i).getConfirm_time() + "','" + list.get(i).getCreate_time() + "','" +list.get(i).getContent() + "','" +
                        list.get(i).getReply_content() + "','" + list.get(i).getZp_content() + "','" + list.get(i).getSkill_score() + "','" + list.get(i).getAttitude_score() + "','" + list.get(i).getEnv_score() + "','" +
                        list.get(i).getSynthesize_score() + "','" + list.get(i).getUpdate_time() + "','" + list.get(i).getIs_zp() + "','" + list.get(i).getIs_reply() + "')");

            }

        }

    }
}

