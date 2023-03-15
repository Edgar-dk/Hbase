package com.sias.hbase.Table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2023-03-15 8:48
 * @faction:
 */
public class DescribeTable {

    private static Connection connection= TestApi.connection;;

    public static void tableDescriptor(String tableName,String cfStr) throws IOException {
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(tableName));
        TableDescriptor tables = table.getDescriptor();
        ColumnFamilyDescriptor columnFamily = tables.getColumnFamily(cfStr.getBytes());
        System.out.println("表"+tableName+"的描述："+tables);
        System.out.println("表"+tableName+"列祖"+cfStr+"的描述："+columnFamily);
    }
    public static void main(String[] args) throws IOException {
        DescribeTable.tableDescriptor("stu1","info");
    }
}
