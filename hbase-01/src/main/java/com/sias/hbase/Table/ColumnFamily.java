package com.sias.hbase.Table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2023-03-15 9:21
 * @faction:
 */

/*增加列祖和删除列祖
* 以及列祖的版本号发生修改*/
public class ColumnFamily {
    public static boolean insertColumnFamily(String tab,String cf) throws IOException {
        Admin admin = TestApi.admin;
        TableName tbName = TableName.valueOf(tab);
        //构造列族描述器
        ColumnFamilyDescriptorBuilder cfnewBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
        //构造列族描述
        ColumnFamilyDescriptor family = cfnewBuilder.build();
        //******新增列族******
        /*tabName是往那个表添加列祖，family是已经构造好的列祖*/
        admin.addColumnFamily(tbName, family);
        return true;
    }
    public static boolean DeleteColumnFamily(String tab,String cf,String ft) throws IOException {
        Admin admin = TestApi.admin;
        TableName tbName = TableName.valueOf(tab);
        //构造列族描述器
        ColumnFamilyDescriptorBuilder cfnewBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
        //修改列族参数
        cfnewBuilder.setMaxVersions(6);

        //构造列族描述
        ColumnFamilyDescriptor family=cfnewBuilder.build();
        //******修改列族******
        admin.modifyColumnFamily(tbName, family);
        // ******删除列族******
        admin.deleteColumnFamily(tbName, Bytes.toBytes(ft));
        return true;
    }

    public static void main(String[] args) throws IOException {
//        boolean b = ColumnFamily.insertColumnFamily("stu1", "kl");
//        System.out.println(b+"增加成功");
        boolean b1 = ColumnFamily.DeleteColumnFamily("stu1", "info", "op");
        System.out.println(b1);
    }
}

