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
public class DeleteData1 {

    public static void DeleteData(String nameSpace, String nameTable, String rowKey) throws IOException {
        Connection connection = TestApi.connection;

        Table table = connection.getTable(TableName.valueOf(nameSpace, nameTable));

        /*04.下面都不带的话，就是删除一行数据*/
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        /*1.删除一个版本的数据
        *   addColumn删除的是指定的列祖里面的列
        *   addColumns删除的是列祖
        *   然后在往上面，是删除的是行数据*/

//        /*01.删除指定的列，最新数据*/
//        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

//        /*02.删除所以版本数据*/
//        delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

//        /*03.删除列祖下所有数据*/
//        delete.addFamily(Bytes.toBytes(columnFamily));

        table.delete(delete);

        TestApi.close();
    }

    public static void main(String[] args) throws IOException {
        DeleteData("default","zhangsan","1001");
        System.out.println("已经删除数据");

    }
}
