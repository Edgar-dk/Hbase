package com.sias.hbase.Table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-27 20:10
 * @faction:
 */
public class AlterTable {

    private static Admin admin=TestApi.admin;
    /*1.修改表格对应的版本号*/
    public static void AlterTable(String nameSpace,String nameTable,String columnFamily,int versions) throws IOException {

        /*0.1在去判断一个表格是否存在
        *    还需要数据库这个前提下*/
        if (!TestApi.tableExec(nameSpace,nameTable)){
            System.out.println("表格不存在");
            return;
        }

        /*1.1获取之前表格信息*/
        TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(nameSpace, nameTable));

        /*1.2注意下面填写的是表格建造者
        *    想要修改之前的信息，需要把之前的表格获取
        *    之后，在填写进去，才可以修改成功，还使用之间的方式
        *    去填写数据的话，就从新创建了一个，那样的话，是不行的*/
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);

        /*1.3获取之前的列祖信息
        *    获取之后，才可以拿着之前的做修改*/
        ColumnFamilyDescriptor columnFamily1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);

        columnFamilyDescriptorBuilder.setMaxVersions(versions);
        tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
        admin.modifyTable(tableDescriptorBuilder.build());
    }


    public static void main(String[] args) throws IOException {
        AlterTable("sias","student1","vds",6);
        System.out.println("修改成功");
        TestApi.close();
    }
}
