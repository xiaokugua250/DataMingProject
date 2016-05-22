package com.morty.java.dmp.Hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by duliang on 2016/5/21.
 */
public class HbaseTableOpt {

    public Configuration hbaseConfiguration;

    //TODO 初始化
    public void init(){
        hbaseConfiguration=  HBaseConfiguration.create();
        // todo settings
        /*hbaseConfiguration.set();

        */
    }

    public Connection getHbaseConnection(Configuration cofiguration){
        try {
            return ConnectionFactory.createConnection(cofiguration);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    //todo hbase pool
    @Deprecated
    public Table hbasePool(String tableName){
        HTablePool pool=new HTablePool();
        Table table=pool.getTable(tableName);
        return table;
    }

    /**
     * HTablePool is Deprecated
     Previous versions of this guide discussed HTablePool, which was deprecated in HBase 0.94, 0.95, and 0.96, and removed in 0.98.1, by HBASE-6500, or HConnection, which is deprecated in HBase 1.0 by Connection.
     Please use Connection instead.
     * table 池
     * try(){} 自动释放资源
     * @param conf
     * @param tableName
     * @return
     * @throws Exception
     */
    public Table hbasePoolCon(Configuration conf, String tableName) throws Exception{

       /* try (Connection connection=ConnectionFactory.createConnection(conf)){
            try(Table table=connection.getTable(TableName.valueOf(tableName))){
                return table;
                // use table as needed, the table returned is lightweight
            }catch (Exception e){
                e.printStackTrace();
            }

        }catch (Exception e){
            e.printStackTrace();
        }*/

        return null;
    }

    //todo hbsae create table

    /**
     *
     * @param name
     * @param famliy
     * @param params
     */
    public void createable(Connection connection,String name,String[] famliy,String ...params){
        try {
            Admin admin= connection.getAdmin();
            HTableDescriptor des=new HTableDescriptor(TableName.valueOf(name));
            HColumnDescriptor[] hcolums=new HColumnDescriptor[famliy.length];
            for(int i=0;i<famliy.length;i++){
                 hcolums[i]=new HColumnDescriptor(famliy[i]);
                des.addFamily(hcolums[i]);
            }
            if(!admin.tableExists(TableName.valueOf(name))){
                    admin.createTable(des); //创建table
            }
        } catch (IOException e) {
            e.printStackTrace();

        }

    }

    /**
     *
     * @param connection
     * @param name
     * @return
     */
    public Table getTable(Connection connection,String name){
        try {
            return connection.getTable(TableName.valueOf(name));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // TODO: 2016/5/21 hbase close table

    /**
     *
     * @param connection
     * @param name
     */
    public void closeTable(Connection connection,String name){

        try {
            Table table=connection.getTable(TableName.valueOf(name));
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
