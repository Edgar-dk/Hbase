package com.sias.hbase.PhoneIx;


import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class Ddl {
    public void list(Connection conn) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//遍历表
    	TableName[] listTableNames = admin.listTableNames();
    	for (TableName tableName : listTableNames) {
			System.out.println(tableName);
		}
    }
    public void createTable(Connection conn,String tabName,String cf) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName name=TableName.valueOf(tabName);
    	//构造表描述器
		TableDescriptorBuilder tabDesBuilder = TableDescriptorBuilder.newBuilder(name);
		
		//构造列族描述器
		ColumnFamilyDescriptorBuilder CFdesBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
		//构造列族描述
		ColumnFamilyDescriptor family=CFdesBuilder.build();
		
		//将列族描述添加到表描述器
		tabDesBuilder.setColumnFamily(family);
		
    	//构造表描述
		TableDescriptor desc=tabDesBuilder.build();
		//创建表
    	admin.createTable(desc);
    }
    
    public void addColumnFamily(Connection conn,String tabName,String cf) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName tableName=TableName.valueOf(tabName);
		
		//构造列族描述器
		ColumnFamilyDescriptorBuilder CFdesBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
		//设置列族参数
		CFdesBuilder.setMaxVersions(3);
		//构造列族描述
		ColumnFamilyDescriptor family=CFdesBuilder.build();

		
		//增加列族
    	admin.addColumnFamily(tableName, family);
    }
    
    public void modifyColumnFamily(Connection conn,String tabName,String cf) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName tableName=TableName.valueOf(tabName);
		
		//构造列族描述器
		ColumnFamilyDescriptorBuilder CFdesBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
		//设置列族参数
		CFdesBuilder.setMaxVersions(2);

		//构造列族描述
		ColumnFamilyDescriptor family=CFdesBuilder.build();

		
		//修改列族
    	admin.modifyColumnFamily(tableName, family);
    }
    
    public void deleteColumnFamily(Connection conn,String tabName,String cf) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName tableName=TableName.valueOf(tabName);
		
		//删除列族
    	admin.deleteColumnFamily(tableName, Bytes.toBytes(cf));
    }
    
    public void desc(Connection conn,String tabName) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName tableName=TableName.valueOf(tabName);
		
		//查看表描述
    	TableDescriptor tabDes = admin.getDescriptor(tableName);
    	System.out.println(tabDes);
    	//查看列族描述
    	ColumnFamilyDescriptor[] columnFamilies = tabDes.getColumnFamilies();
    	System.out.println("NAME"+"\t"+"VERSIONS");
    	for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilies) {
			System.out.println(columnFamilyDescriptor.getNameAsString()+"\t"+columnFamilyDescriptor.getMaxVersions());
		}
    }
    
    public void disableTable(Connection conn,String tabName) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName tableName=TableName.valueOf(tabName);
		
		//让表失效
    	admin.disableTable(tableName);
    	System.out.println(tabName+" 是否有效： "+admin.isTableEnabled(tableName));

    }
    
    public void truncateTable(Connection conn,String tabName) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName tableName=TableName.valueOf(tabName);
		
    	//清空表要先让表失效
    	admin.disableTable(tableName);
		//清空表
    	admin.truncateTable(tableName, true);
    }
    
    public void tableExists(Connection conn,String tabName) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName tableName=TableName.valueOf(tabName);
		
    	//表的存在性判断
    	System.out.println(tabName+" 是否存在： "+admin.tableExists(tableName));
    }
    
    public void drop(Connection conn,String tabName) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造表名对象
    	TableName tableName=TableName.valueOf(tabName);
		
    	//删除表也要先让表失效
    	admin.disableTable(tableName);
    	//删除表
    	admin.deleteTable(tableName);
    }
    
    public void createNamespace(Connection conn,String nsName) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();
    	//构造命名空间对象
    	NamespaceDescriptor descriptor = NamespaceDescriptor.create(nsName).build();

		//创建命名空间
    	admin.createNamespace(descriptor);
    }
    
    public void deleteNamespace(Connection conn,String nsName) throws Exception {
    	//获取HBase管理对象
    	Admin admin = conn.getAdmin();

		//创建命名空间 要先删除里面包含的表
    	admin.deleteNamespace(nsName);
    }
}
