package com.cassandra.keyspace;

import com.datastax.driver.core.Session;

public class KeySpace {
	public void createKeySpace(Session session , String keyName,int replication) {
		String query = "CREATE KEYSPACE "+keyName+" WITH replication "+"= {'class':'SimpleStrategy','replication_factor':"+replication+"}; ";
		try {
			session.execute(query);
			session.execute("USE "+keyName);
			
			System.out.println("keySpace Created");
		}catch(Error err) {
			err.printStackTrace();
		}
	}
	
	public void deleteKeySpace(Session session, String keyName) {
		String query = "DROP KEYSPACE "+keyName+";";
		try {
			session.execute(query);
			System.out.println("Deleted successfully");
		}catch(Error err) {
			err.printStackTrace();
		}
	}


}
