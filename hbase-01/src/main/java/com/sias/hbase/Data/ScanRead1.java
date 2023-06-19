package com.sias.hbase.Data;

import com.sias.hbase.Table.TestApi;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-28 13:44
 * @faction:
 */
public class ScanRead1 {

    public static void ScanRead(String nameSpace, String nameTable, String startRow, String endRow) throws IOException {
        Connection connection = TestApi.connection;
        Table table = connection.getTable(TableName.valueOf(nameSpace, nameTable));
        /*1.左闭，右开*/
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(endRow));
        scan.addFamily("info".getBytes());
        /*2.先从获取过来的table去扫描对应的数据、
        * 发现里面缺少一个对象scan，明确显示，需要在这个对象
        * 里面添加上数据之后，在放进参数中*/
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            /*2.1在cell里面数据是一行里面有多个数据，
            *    因为这个是对应到列祖进行的，一个列祖有
            *    多个列，在去考虑的时候，也需要用for
            *    循环，把数据一个一个的打出来
            *    cloneRow是行号，cloneFamily是列祖名字，cloneQualifier是列祖下面的列的名字
            *    cloneValue是这个列下面对应的值，一个cell是一个列下面的值，一行有多个列，所以是
            *    for循环cell*/
            for (Cell cell : cells) {
                System.out.print(new String(CellUtil.cloneRow(cell)) + "_" +
                        new String(CellUtil.cloneFamily(cell)) + "_" + new String(CellUtil.cloneQualifier(cell))
                                + "_" + new String(CellUtil.cloneValue(cell))+"\t");
            }
            System.out.println();
        }
        TestApi.close();
    }
    public static void main(String[] args) throws IOException {
        ScanRead("default",
                "zhangsan",
                "1001",
                "1003");
    }
}
