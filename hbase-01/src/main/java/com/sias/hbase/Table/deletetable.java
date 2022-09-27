package com.sias.hbase.Table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-27 21:06
 * @faction:
 */
public class deletetable {

    private static Admin admin=TestApi.admin;

    public static boolean deleteTable(String nameSpace,String nameTable) throws IOException {

        /*1.不存在的话，返回一个false
        *   存在的话，在往下面执行*/
        if (!TestApi.tableExec(nameSpace,nameTable)){
            System.out.println("表格不存在，不能删除");
            return false;
        }

        /*2.在去删除之前，需要把表格弃用*/
        admin.disableTable(TableName.valueOf(nameSpace,nameTable));
        admin.deleteTable(TableName.valueOf(nameSpace,nameTable));

        TestApi.close();
        return true;
    }

    public static void main(String[] args) throws IOException {
        deleteTable("sias","student1");
        System.out.println("删除表格成功");
    }
}
