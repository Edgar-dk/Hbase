package com.sias.hbase.Data;

import com.sias.hbase.Table.TestApi;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-28 16:50
 * @faction:
 */
public class DeleteData {

    public static void DeleteData(String nameSpace, String nameTable, String rowKey, String columnFamily, String columnName) throws IOException {
        Connection connection = TestApi.connection;
        Table table = connection.getTable(TableName.valueOf(nameSpace, nameTable));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        /*1.删除一个版本的数据
        *   addcolums删除多个版本*/
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        table.delete(delete);

        TestApi.close();
    }

    public static void main(String[] args) throws IOException {
        DeleteData("default","stu","1001","info","name");
        System.out.println("已经删除数据");

    }
}
