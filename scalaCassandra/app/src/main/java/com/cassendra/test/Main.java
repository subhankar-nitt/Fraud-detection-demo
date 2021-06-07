package com.cassendra.test;

import com.cassandra.data.DataOperation;
import com.cassandra.table.Table_Operations;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Main {

	public static void main(String[] args) {
		
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect();
		new DataOperation().createData(session, "tp", cluster, "iris");
	}
	
}
