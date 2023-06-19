package com.sias.hbase.PhoneIx;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Dml {
    public void put(Connection conn,String tabName) throws Exception {
    	//获取表名对象
    	TableName tableName=TableName.valueOf(tabName);
    	//获取表对象，访问数据的入口
		Table table = conn.getTable(tableName);
//		//构造put对象
//		Put put=new Put(Bytes.toBytes("1001"));
//		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes("2"));
//		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
//		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("addr"), Bytes.toBytes("上海"));
//		
//		//新增、修改数据
//		table.put(put);
		
		//构造批处理的LIST列表
		List<Put> puts=new ArrayList<Put>();
		//构造put对象
		Put put1=new Put(Bytes.toBytes("1002"));
		put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes("2001"));
		put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
		put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("addr"), Bytes.toBytes("guangzhou"));
		//追加LIST的元素
		puts.add(put1);
		
		Put put2=new Put(Bytes.toBytes("1003"));
		put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes("3001"));
		put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
		put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("addr"), Bytes.toBytes("shenzhen"));
		//追加LIST的元素
		puts.add(put2);
		
		
		//批量增加、修改数据
		table.put(puts);
    }
    
    public void get(Connection conn,String tabName) throws Exception {
    	//获取表名对象
    	TableName tableName=TableName.valueOf(tabName);
    	//获取表对象，访问数据的入口
		Table table = conn.getTable(tableName);
		//构造get对象
		Get get=new Get(Bytes.toBytes("1002"));
		//过滤到列族
//		get.addFamily(Bytes.toBytes("info"));
		//过滤到列
		get.addColumn(Bytes.toBytes("other"), Bytes.toBytes("email")).readVersions(3).setTimeRange(1678955552416L, 1678955557396L);
		// get查询数据
		Result result = table.get(get);
		System.out.println("rowkey"+"\t"+"CF"+"\t"+"Qualifier"+"\t"+"Value");
		//解析结果集
		for (Cell cell : result.listCells()) {
//			System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
//					+"\t"+Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
//					+"\t"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
//					+"\t"+Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
//					);
			
			System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
					+"\t"+Bytes.toString(CellUtil.cloneFamily(cell))
					+"\t"+Bytes.toString(CellUtil.cloneQualifier(cell))
					+"\t"+Bytes.toString(CellUtil.cloneValue(cell))
					);
			
		}
    }
    
    public void scan(Connection conn,String tabName) throws Exception {
    	//获取表名对象
    	TableName tableName=TableName.valueOf(tabName);
    	//获取表对象，访问数据的入口
		Table table = conn.getTable(tableName);
		//构造san对象
		Scan scan=new Scan();
		//高级选项
		scan.setRaw(true).readAllVersions();
		//过滤到列族
//		scan.addFamily(Bytes.toBytes("info"));
		//过滤到列
//		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")).withStartRow(Bytes.toBytes("1002")).withStopRow(Bytes.toBytes("1003"), true);
		// scan数据
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("rowkey"+"\t"+"CF"+"\t"+"Qualifier"+"\t"+"Value");
		//解析scan结果集
		for (Result result : scanner) {
			//解析行结果集
			for (Cell cell : result.listCells()) {
//				System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
//						+"\t"+Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
//						+"\t"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
//						+"\t"+Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
//						);
				
				System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
						+"\t"+Bytes.toString(CellUtil.cloneFamily(cell))
						+"\t"+Bytes.toString(CellUtil.cloneQualifier(cell))
						+"\t"+Bytes.toString(CellUtil.cloneValue(cell))
						);
				
			}
		}
		
		
    }
    
    public void delete(Connection conn,String tabName) throws Exception {
    	//获取表名对象
    	TableName tableName=TableName.valueOf(tabName);
    	//获取表对象，访问数据的入口
		Table table = conn.getTable(tableName);
		//构造delete对象
		Delete delete=new Delete(Bytes.toBytes("1001"));
		//精确到列级,最新版本数据
//		delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"));
		//删除列的多版本信息
//		delete.addColumns(Bytes.toBytes("info"), Bytes.toBytes("id"));
		//删除列族的数据 *****
//		delete.addFamily(Bytes.toBytes("other"));
		
		
		//执行删除
		table.delete(delete);
		
    }
    
}
