package com.nswdwy.kylin;

/**
 * @author yycstart
 * @create 2020-11-18 18:47
 */
import java.sql.*;

public class TestKylin {

    public static void main(String[] args) throws Exception {

        //Kylin_JDBC 驱动
        String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";

        //Kylin_URL
        String KYLIN_URL = "jdbc:kylin://hadoop102:7070/gmall";

        //Kylin的用户名
        String KYLIN_USER = "ADMIN";

        //Kylin的密码
        String KYLIN_PASSWD = "KYLIN";

        //添加驱动信息
        Class.forName(KYLIN_DRIVER);

        //获取连接
        Connection connection = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWD);

        //预编译SQL
        PreparedStatement ps = connection.prepareStatement("select\n" +
                "    bp.PROVINCE_NAME,\n" +
                "    ui.GENDER,\n" +
                "    sum(pi.PAYMENT_AMOUNT) PAYMENT_AMOUNT\n" +
                "from DWD_FACT_PAYMENT_INFO pi\n" +
                "join DWD_DIM_BASE_PROVINCE bp\n" +
                "on pi.PROVINCE_ID=bp.id\n" +
                "join DWD_DIM_USER_INFO_HIS_VIEW ui\n" +
                "on pi.user_id=ui.id\n" +
                "group by bp.PROVINCE_NAME,ui.GENDER;");

        //执行查询
        ResultSet resultSet = ps.executeQuery();

        //遍历打印
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1)+":"+resultSet.getString(2)+"-"+resultSet.getInt(3));
        }
    }
}

