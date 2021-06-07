package com.cassandra.data;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class DataOperation {
	public void createData(Session session, String clusterName, Cluster cluster, String tableName) {
		String query = "INSERT INTO "+tableName+"(id,petal_len,petal_width,sepal_len,sepal_width,species) VALUEs(1,'1.2','2.4','2.4','4.5','SPECIES');";
		try {
		cluster.connect(clusterName);
		session.execute("USE "+clusterName);
		session.execute(query);
		System.out.println("Inserted Successfully");
		
	}
	catch(Exception ex) {
		ex.printStackTrace();
	}
  }

}
