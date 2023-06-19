package com.sias.hbase.PhoneIx;

import org.apache.phoenix.shaded.org.apache.curator.framework.schema.SchemaViolation;

import java.sql.*;

public class Phoenix {
	public void query(Connection conn) throws Exception {
		//构造SQL
		String sql="select * from stu where to_number(id)>?";
		//预编译
		PreparedStatement pst = conn.prepareStatement(sql);
		//绑定变量
		pst.setObject(1, 3);
		//执行SQL
		ResultSet res = pst.executeQuery();
		//解析结果集
		while(res.next()) {
			System.out.println(res.getObject(1)+"\t"+res.getObject(2)+"\t"+res.getObject(3));
		}
		
		//释放资源
		res.close();
		pst.close();
	}

	public void query1(Connection connection) throws SQLException {
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("select * from tab");

		while (resultSet.next()){
			String id = resultSet.getString("id");
			String name = resultSet.getString("name");
			System.out.println("id:"+id+"name:"+name);
		}
		resultSet.close();
		statement.close();
		connection.close();


	}

	public void query2(Connection connection) throws SQLException {
		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery("select * from tab");

		while (resultSet.next()){
			String id = resultSet.getString("id");
			String name = resultSet.getString("name");
			System.out.println("id:"+id+"name:"+name);
		}

		resultSet.close();
		statement.close();
		connection.close();

	}
	
	public void upsert(Connection conn) throws Exception {
		/*1.分析，对于这里面的conn而言，这个是conn已经连接了Hbase中Phone的地址
		*        因此在这里直接调用下面的方法，把语句，以及要插入的数据放进去就可以了*/
		//构造SQL
		String sql="upsert into stu values(?,?,?)";
		//预编译
		PreparedStatement pst = conn.prepareStatement(sql);
		//绑定变量
		pst.setObject(1, "666");
		pst.setObject(2, "wwwUpate");
		pst.setObject(3, 20);
		//执行SQL
		int res = pst.executeUpdate();
		System.out.println(conn.getAutoCommit());
		//显式提交
		conn.commit();
		System.out.println(res+" 条数据被UPSERT");
		
		//释放资源
		pst.close();
	}

	public void upp(Connection connection) throws SQLException {

		Statement statement = connection.createStatement();
		statement.executeUpdate("upsert into tab values('1','Alice')");
		statement.executeUpdate("upsert into tab values('2','Ff')");
		statement.executeUpdate("upsert into tab values('3','POJ')");

		statement.executeUpdate("upsert into tab values('1','Alices')");
		connection.commit();
		statement.close();
		connection.close();

	}

	public void upp1(Connection connection) throws SQLException {
		Statement statement = connection.createStatement();

		statement.executeUpdate("upsert into tab values('1','Alice')");
		statement.executeUpdate("upsert into tab values('2','Plice')");
		statement.executeUpdate("upsert into tab values('3','Ksi')");

		statement.executeUpdate("upsert into tab values('1','KF') ");

		connection.commit();
		statement.close();
		connection.close();
	}


	public void delete(Connection conn) throws Exception {
		//构造SQL
		String sql="delete from stu where id=?";
		//预编译
		PreparedStatement pst = conn.prepareStatement(sql);
		//绑定变量
		pst.setObject(1, "666");

		//执行SQL
		int res = pst.executeUpdate();

		//显式提交
		conn.commit();
		
		System.out.println(res+" 条数据被delete");
		
		//释放资源
		pst.close();
	}
	
	public void ddl(Connection conn,String tabName) throws Exception {
		//构造SQL
		String sql="create table "+tabName+"(id integer primary key,name varchar(50))";
		//预编译
		PreparedStatement pst = conn.prepareStatement(sql);

		//执行SQL
		int res = pst.executeUpdate();

		System.out.println(res);
		
		//释放资源
		pst.close();
	}
	
}
