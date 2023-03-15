package com.sias.hbase.Data;

import com.sias.hbase.Table.TestApi;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
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
public class Truncate {
    private static Connection connection= TestApi.connection;

    public static void truncateTable(String tableName, boolean pre) throws IOException {
        Admin admin = TestApi.admin;
        admin.disableTable(TableName.valueOf(tableName));
        admin.truncateTable(TableName.valueOf(tableName),pre);
    }

    public static void main(String[] args) throws IOException {
        Truncate.truncateTable("o",true);
        System.out.println("清空数据成功");
    }
}
