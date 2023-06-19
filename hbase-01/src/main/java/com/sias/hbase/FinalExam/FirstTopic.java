package com.sias.hbase.FinalExam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.org.glassfish.hk2.utilities.binding.ScopedNamedBindingBuilder;

import java.io.IOException;

/**
 * @author Edgar
 * @create 2023-06-18 18:12
 * @faction:
 */
public class FirstTopic {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            Table tab = connection.getTable(TableName.valueOf("tab"));
            Scan scan = new Scan();
            Scan cd = scan.addFamily(Bytes.toBytes("cd"));
            ResultScanner scanner = tab.getScanner(scan);
            for (Result result : scanner) {
                byte[] id = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id"));
                byte[] name = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name"));
                System.out.println("RowKey:"+Bytes.toString(result.getRow())+
                        ",id"+Bytes.toString(id)+
                        ",name"+ Bytes.toString(name));
            }
            scanner.close();
            tab.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
