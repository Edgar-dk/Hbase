package com.sias.hbase.Table;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-27 14:58
 * @faction:
 */

/*创建命名空间*/

public class CreateNameSpace {

    private static Admin admin=TestApi.admin;

    /*1.创建命名空间*/
    public static void createNameSpace(String nameSpace) throws IOException {

        /*1.获取一个建造者设计师
        *   有了这个设计师就可以去
        *   根据不同场景的需求，填写对应的参数*/
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);

        /*2.添加描述
        *   给这个数据库（namespace）
        *   添加的描述*/
        builder.addConfiguration("user","ng");

        /*3.下面会返回对上面信息的描述
        *   下面调用了一个build是把namespace
        *   的内容以及描述填写进去了*/
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("数据库已存在");
            e.printStackTrace();
        }
        TestApi.close();
    }


    /*2.创建表格*/
    public static void createTable(String nameSpace,String nameTable,String... columnFamilies) throws IOException {
        if (columnFamilies.length == 0){
            System.out.println("列祖数量不能等于0");
            return;
        }

        if (TestApi.tableExec(nameSpace,nameTable)){
            System.out.println("表格已经存在");
            return;
        }


        /*2.1创建表格描述建造者
        *    在去创建这个表格描述的时候，可以在后面
        *    添加上指定的命名空间，以及表的名字*/
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, nameTable));

        /*2.1.1由于列祖是多个的
        *      所以需要去遍历的方式去吧列祖信息
        *      添加进列祖建造者中，因为列祖建造者和表格建造者是两个东西*/
        for (String columnFamily : columnFamilies) {
            /*2.2创建列祖描述建造者*/
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            /*2.3添加版本号*/
            columnFamilyDescriptorBuilder.setMaxVersions(5);

            /*2.4将列祖信息添加进入表格里面
            *    使用建造者模式的方式去构建数据*/
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        /*3.然后在把数据放在表里面*/
        admin.createTable(tableDescriptorBuilder.build());
        TestApi.close();
    }


    /*3.演示上面方法执行*/
    public static void main(String[] args) throws IOException {
//        createNameSpace("sias");
        createTable("sias","student1","vds");
        System.out.println("其他代码");
    }
}
