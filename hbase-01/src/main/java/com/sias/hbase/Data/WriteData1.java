package com.sias.hbase.Data;

import com.sias.hbase.Table.TestApi;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author Edgar
 * @create 2022-09-27 21:22
 * @faction:
 */
public class WriteData1 {
    private static Connection connection= TestApi.connection;

    public static void insertData(String nameSpace,String nameTable) throws IOException, InterruptedException {

        Table table = connection.getTable(TableName.valueOf(nameSpace, nameTable));


        Put put1 = new Put(Bytes.toBytes("1001"));
        Put put2 = new Put(Bytes.toBytes("1002"));
        Put put3 = new Put(Bytes.toBytes("1003"));



        put1.addColumn("info".getBytes(),"id".getBytes(),"1".getBytes());
        put1.addColumn("info".getBytes(),"name".getBytes(),"jack".getBytes());
        put1.addColumn("other".getBytes(),"addr".getBytes(),"beijing".getBytes());
        put1.addColumn("other".getBytes(),"email".getBytes(),"1@jack.com".getBytes());



        put2.addColumn("info".getBytes(),"id".getBytes(),"2".getBytes());
        put2.addColumn("info".getBytes(),"name".getBytes(),"rose".getBytes());
        put2.addColumn("other".getBytes(),"addr".getBytes(),"shanghai".getBytes());
        put2.addColumn("other".getBytes(),"email".getBytes(),"1@rose.com".getBytes());



        put3.addColumn("info".getBytes(),"id".getBytes(),"3".getBytes());
        put3.addColumn("info".getBytes(),"name".getBytes(),"tom".getBytes());
        put3.addColumn("other".getBytes(),"addr".getBytes(),"guangzhou".getBytes());
        put3.addColumn("other".getBytes(),"email".getBytes(),"1@tom.com".getBytes());

        ArrayList<Put> puts = new ArrayList<>();
        puts.add(put1);
        puts.add(put2);
        puts.add(put3);

        table.put(puts);
        TestApi.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        WriteData1.insertData("default","zhangsan");
        System.out.println("插入数据成功");
    }
}
