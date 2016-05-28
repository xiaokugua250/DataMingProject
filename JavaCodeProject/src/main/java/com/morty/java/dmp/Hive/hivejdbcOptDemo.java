package com.morty.java.dmp.Hive;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Properties;

/**
 * Created by morty on 2016/05/18.
 */
public class HiveJdbcOptDemo {
    static Properties properties;
    Logger LOG = Logger.getLogger(HiveJdbcOptDemo.class);

    public static void init(){
        properties=new Properties();
        properties.put("user", HiveInfo.hiveuser);
        properties.put("password", HiveInfo.hivepassword);
        properties.put("table", HiveInfo.hiveTableName);
    }
    /**
     *
     * @param driver
     * @param user
     * @param password
     * @return
     */
    public static boolean getConnect(String driver,String user,String password){
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
     *
     * @param statement
     * @param sql
     * @param sqlType  true 带返回值，false 不带返回值
     * @parm params  sql执行时的参数
     */
    public static void sqlExeuteOpt(Statement statement,String sql,boolean sqlType,String ...params)  {
        if(sqlType == true){
            try {
                ResultSet resultSet=statement.executeQuery(sql);
                while (resultSet.next()){
                    //// TODO: 2016/05/18  查询结构操作

                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else if(sqlType == false){
            try {
                //// TODO: 2016/05/18  要执行的sql excuteXX(sql)
                statement.execute(sql,params);
            } catch (SQLException e) {
                e.printStackTrace();
            }
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
