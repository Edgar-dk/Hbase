package com.sias.hbase.Data;

import com.sias.hbase.Table.TestApi;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-28 11:14
 * @faction:
 */
public class ReadDate {


    public static void readData(String nameSpace,String nameTable,String rowKey,String columnFamily,String columnName) throws IOException {
        Connection connection = TestApi.connection;
        Table table = connection.getTable(TableName.valueOf(nameSpace, nameTable));
        /*1.在下面直接写上这一行数据的话
        *   读取的是一行数据，想要做数据的精确，
        *   可以用get指定具体的列，在读取版本数据的时候
        *   里面设置了几个版本的数据，就可以读取到几个，
        *   假如说，这一个字段设置成了，2个版本，读取的时候，就可以读取
        *   到两个版本*/
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        get.readAllVersions();
        /*2.读取数据，得到的是一个result
        *   里面的数据还不能去读取，需要处理*/
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        /*3.遍历数据想要处理数据的话，也是需要
        *   用到特定的方式去处理，把数据格式化成字符串
        *   ，这个地方是测试环境，在实际的开发中，需要用到
        *   把数据转移到一个地方，需要另外一个放处理
        *   在去打印数据的时候，是从之前的数据里面复制过来一份，
        *   然后在把复制过来的数据打印出来*/
        for (Cell cell : cells) {
            String values = new String(CellUtil.cloneValue(cell));
            System.out.println(values);
        }
        TestApi.close();
    }
    public static void main(String[] args) throws IOException {
        readData("default","stu1","1001","eo","name");
        System.out.println("数据查询成功");
    }
}
