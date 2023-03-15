package com.sias.hbase.Data;

import com.sias.hbase.Table.TestApi;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2023-03-15 9:33
 * @faction:
 */

/*查看表中的某一行数据是否存在*/
public class JudgeData {

    public static Boolean ViewData(String tableName, String rowkey) throws IOException {
        Connection conn = TestApi.connection;
        // 表对象
        TableName tbName = TableName.valueOf(tableName);
        // 获取表的实例
        Table table = conn.getTable(tbName);
        Get get = new Get(rowkey.getBytes());
        // 判断行的数据是否存在
        Boolean existRowFlag = table.exists(get);
        System.out.println("表 " + tableName + " 指定行 " + rowkey + " 的数据是否存在："+ existRowFlag);
        return existRowFlag;
    }
    public static void main(String[] args) throws IOException {
        Boolean stu1 = JudgeData.ViewData("stu1", "1004");
        System.out.println(stu1+"不存在");
    }
}
