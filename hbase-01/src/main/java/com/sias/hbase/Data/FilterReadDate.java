package com.sias.hbase.Data;

import com.sias.hbase.Table.TestApi;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import sun.awt.SunHints;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-28 11:14
 * @faction:
 */
public class FilterReadDate {

    public static void ScanRead(String nameSpace, String nameTable, String startRow, String endRow,String columnFamily,String columnName,
                                String value) throws IOException {
        Connection connection = TestApi.connection;
        Table table = connection.getTable(TableName.valueOf(nameSpace, nameTable));
        /*1.左闭，右开*/
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(endRow));

        /*2.把想要过滤的条件数据，筛选出来
        *   只保留单列的数据*/
        FilterList filterList = new FilterList();
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName),
                CompareOperator.EQUAL,
                Bytes.toBytes(value)

        );
        filterList.addFilter(columnValueFilter);
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
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
                "stu",
                "1001",
                "1003",
                "info",
                "name",
                "zhangsan");
    }
}
