package com.sias.hbase.Data;

import com.sias.hbase.Table.TestApi;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-27 21:22
 * @faction:
 */
public class AlterData {
    private static Connection connection= TestApi.connection;

    public static void insertData(String nameSpace,String nameTable,String rowKey,String columnFamily,String columnName,String value) throws IOException {
        /*1.从nameSpace里面获取表格信息*/
        Table table = connection.getTable(TableName.valueOf(nameSpace, nameTable));
        /*2.rowKey是一行数据假如说是1001这一行*/
        Put put = new Put(Bytes.toBytes(rowKey));
        /*3.在一行里面添加
            对应的列祖以及字段，还有值
            在一行里面的列祖，下面的列，添加value值*/
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));
        /*4.然后把信息放入表格里面*/
        table.put(put);
        TestApi.close();
    }
    /*1.修改数据
    *   只需要在原先的基础上，还是用put就是把之前的数据覆盖掉了，也就是修改了数据*/
    public static void main(String[] args) throws IOException {
        AlterData.insertData("default","stu1","1001","info","name","zhang");
        System.out.println("修改数据");
    }
}
