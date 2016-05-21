package com.morty.java.dmp.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import scala.util.control.Exception;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by duliang on 2016/5/21.
 *
 * data crud operation
 */
public class hbaseDataOpt {
    public Configuration hbaseConfiguration;
    public void init(){

    }


    /**
     *
     * @param table
     * @param tableName
     * @param rowName
     * @param colFamily
     * @param col
     * @param data
     * @param params
     */
    public void putAndUpdateData(Table table,String tableName, String rowName, String colFamily, String col, String data,String ... params){

        try {
            Put put=new Put(Bytes.toBytes(rowName));
            put.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col),Bytes.toBytes(data));
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }



    /**
     *
     * @param table
     * @param tableName
     * @param rowName
     * @param colFamily
     * @param col
     */
    public void delData(Table table,String tableName,String rowName,String colFamily,String col){
        try {
            Delete delete=new Delete(Bytes.toBytes(rowName));
            delete.addFamily(Bytes.toBytes(colFamily)); //删除列族
            delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));     //删除列
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    /**
     *
     * @param table
     * @param rowName
     * @param colFamily
     * @param col
     */
    public void getData(Table table,String rowName,String colFamily,String col){

        try {
            Get get=new Get(Bytes.toBytes(rowName));
            Result result=table.get(get);
            byte[] rb=result.getValue(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            String value=new String(rb,"utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     *
     * @param table
     * @throws IOException
     */
    public void getDataAll(Table table) throws IOException{

        Scan scan=new Scan();
        ResultScanner resultScanner=table.getScanner(scan);

        try {
            for(Result result:resultScanner){
                List<Cell> cells=result.listCells();
                for(Cell cell:cells){
                    byte[] rb=cell.getValueArray();
                    String row=new String(result.getRow(),"utf-8");
                    String family=new String(CellUtil.cloneFamily(cell),"utf-8");
                    String qualifier=new String(CellUtil.cloneQualifier(cell),"utf-8");
                    String value=new String(CellUtil.cloneValue(cell),"utf-8");
                }
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


    }


}
