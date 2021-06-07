package com.cassandra.table;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Table_Operations {
	public void createTable(Session session,String clusterName,Cluster cluster ) {
		String query ="CREATE TABLE iris(id int PRIMARY KEY, petal_len text , petal_width text,sepal_len text,sepal_width text,species text);";
		try {
			
			cluster.connect(clusterName);
			session.execute("USE "+clusterName);
			session.execute(query);
			System.out.println("Table created successfully");
		}catch(Exception ex) {
			ex.printStackTrace();
		}
		
		
	}
}
