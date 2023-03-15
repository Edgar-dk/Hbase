package com.sias.hbase.Table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-09-26 21:09
 * @faction:
 */
public class BaseOption {
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

    /*3.表失效*/
    public static boolean DisTable(String nameSpace,String tableName) throws IOException {
        /*1.不存在的话，返回一个false
         *   存在的话，在往下面执行*/
        if (!TestApi.tableExec(nameSpace,tableName)){
            System.out.println("表格不存在，不能删除");
            return false;
        }
        admin.disableTable(TableName.valueOf(nameSpace,tableName));
        return true;
    }
    /*4.表有效*/
    public static boolean EnbTable(String nameSpace,String tableName) throws IOException {
        /*1.不存在的话，返回一个false
         *   存在的话，在往下面执行*/
        if (!TestApi.tableExec(nameSpace,tableName)){
            System.out.println("表格不存在，不能删除");
            return false;
        }
        admin.enableTable(TableName.valueOf(nameSpace,tableName));
        return true;
    }

    /*4.执行，查看表是否存在*/
    public static void main(String[] args) throws IOException {
        /*01.失效*/
        /*boolean b = BaseOption.DisTable("default", "stu1");
        System.out.println(b);*/

        /*02.有效*/
        boolean b1 = BaseOption.EnbTable("default", "stu1");
        System.out.println(b1);
        /*03.获取表的描述信息*/
//        TableDescriptor stu1 = BaseOption.descriptor("stu1","info");
//        System.out.println(stu1);
        close();
    }

}
