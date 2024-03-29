package com.sias.hbase.Table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-26 21:09
 * @faction:
 */
public class TestApi {
    public static Connection connection = null;
    public static Admin admin = null;

    /*1.配置信息*/
    static {
        /*1.获取配置文件信息，
         *   就是zk的配置信息
         *   可以把获取参数的配置信息，写在这个下面
         *   就是下面注释的信息，也可以写在resource里面
         *   在去读取信息的时候，直接加载这个resource信息里面
         *   的配置信息*/
        /*Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104"); */
        /*2.由工厂对象去创建一个连接，
         *   然后把连接的信息放在工厂里面，信息不可能一下子都
         *   放在这个工厂里面，先把信息放在一个加载信息的对象中，然后
         *   在把这个对象放在工厂中，工厂是加载连接的驱动。
         *    ，获取到Hbase对象之后，就得到了这个Hbase的权限，就可以去
         *   在Hbase里面操作表，或者是其他的数据操作
         *
         *   admin的连接是轻量级的，不是线程安全的，不推荐缓存这个连接，使用的时候，拿过来
         *   不使用的话，关闭即可*/
        try {
            connection = ConnectionFactory.createConnection();
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*2.关闭连接*/
    public static void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /*3.查看表是否存在*/
    public static boolean tableExec(String nameSpace,String tableName) throws IOException {
        boolean exit = admin.tableExists(TableName.valueOf(nameSpace,tableName));
        return exit;
    }

    /*4.列出所有的表*/
    public static TableName[] listTable() throws IOException {
        TableName[] tableNames = admin.listTableNames();
        return tableNames;
    }
    /*4.执行，查看表是否存在*/
    public static void main(String[] args) throws IOException {
        /*01.查看表是否存在*/
//        System.out.println(tableExec("default","stu1") + "成功");

        /*02.列出所有的表*/
        TableName[] tableNames = listTable();
        for (TableName tableName : tableNames) {
            System.out.println(tableName);
        }
        close();
    }

}
