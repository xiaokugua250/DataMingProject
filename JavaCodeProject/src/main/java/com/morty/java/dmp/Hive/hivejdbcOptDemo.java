package com.morty.java.dmp.Hive;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Properties;

/**
 * Created by morty on 2016/05/18.
 */
public class HivejdbcOptDemo {
    static Properties properties;
    Logger            LOG = Logger.getLogger(HivejdbcOptDemo.class);

    public static void init() {
        properties = new Properties();
        properties.put("user", HiveInfo.hiveuser);
        properties.put("password", HiveInfo.hivepassword);
        properties.put("table", HiveInfo.hiveTableName);
    }

    /**
     *
     * @param statement
     * @param sql
     * @param sqlType  true ������ֵ��false ��������ֵ
     * @parm params  sqlִ��ʱ�Ĳ���
     */
    public static void sqlExeuteOpt(Statement statement, String sql, boolean sqlType, String... params) {
        if (sqlType == true) {
            try {
                ResultSet resultSet = statement.executeQuery(sql);

                while (resultSet.next()) {

                    //// TODO: 2016/05/18  ��ѯ�ṹ����
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else if (sqlType == false) {
            try {

                //// TODO: 2016/05/18  Ҫִ�е�sql excuteXX(sql)
                statement.execute(sql, params);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param driver
     * @param user
     * @param password
     * @return
     */
    public static boolean getConnect(String driver, String user, String password) {
        try {
            Class.forName(HiveInfo.hiveJdbcClientDriveName);

            Connection connection = DriverManager.getConnection(HiveInfo.hiveUri, properties);

            return true;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();

            return false;
        } catch (SQLException e) {
            e.printStackTrace();

            return false;
        }
    }

    /**
     * @param connection
     * @return
     */
    public Statement getStatement(Connection connection) {
        try {
            Statement statement = connection.createStatement();

            return statement;
        } catch (SQLException e) {
            e.printStackTrace();

            return null;
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
