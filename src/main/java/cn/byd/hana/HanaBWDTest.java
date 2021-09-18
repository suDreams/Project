package cn.byd.hana;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.sql.*;
import java.util.List;

/**
 * Created by huang.hai7 on 2021/7/30.
 */
public class HanaBWDTest {
    private static final String DRIVER = "com.sap.db.jdbc.Driver";

    private static final String USER = "WUMIAOMIAO";

    private static final String PWD = "Byd;2106";

    private static final String URL = "jdbc:sap://192.168.100.204:30015?reconnect=true";

    //    private static final String SQL = "select * from \"SAPABAP1\".\"/BIC/AZEZJT3272\"";
    private static final String SQL = "select * from \"SAPABAP1\".\"/BIC/AZEZJT3572\"";



    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER);

        System.out.println("Start Connection.");

        Connection conn = DriverManager.getConnection(URL, USER, PWD);

        System.out.println(SQL);

        PreparedStatement pStmt = conn.prepareStatement(SQL);

        ResultSet rs = pStmt.executeQuery();

        while (rs.next()) {
            ResultSetMetaData rsMetaData = rs.getMetaData();

            int colNum = rsMetaData.getColumnCount();

            for (int i = 1; i <= colNum; i++) {
                String val = rs.getString(i);
                System.out.println(val);
            }
        }
    }

}