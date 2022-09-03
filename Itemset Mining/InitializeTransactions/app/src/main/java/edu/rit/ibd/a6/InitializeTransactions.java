package edu.rit.ibd.a6;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mysql.jdbc.PreparedStatement;

public class InitializeTransactions {

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
		
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoCol);  // all those documents in this one.
		
		
		
		java.sql.PreparedStatement pst = con.prepareStatement(sqlQuery);
		pst.setFetchSize(/* Batch size */ 50000);
		ResultSet rs = pst.executeQuery();
		System.out.print("SQL started " + new Date());
		HashMap<Integer, ArrayList<Integer>> hm = new HashMap<>();
		while(rs.next()) {
			Integer ttId = rs.getInt("tid");
			Integer iId = rs.getInt("iid");
			ArrayList<Integer> l;
			if(hm.containsKey(ttId)) {
				 l = hm.get(ttId);
			}else {
				 l = new ArrayList<>();
			}
			l.add(iId);
			hm.put(ttId, l);

		}
		//for loop
		ArrayList<Document> list = new ArrayList<>();
		int count = 0;
		for(Map.Entry<Integer, ArrayList<Integer>> entry : hm.entrySet()) {
		    Integer key = entry.getKey();
		    ArrayList<Integer>  value = entry.getValue();
		    Document d = new Document();
		    d.append("_id", key);
		    d.append("items", value);
			list.add(d);
			count++;
			if(count % 5000 == 0) {
				System.out.println("\n-------------inserted---------------");
				transactions.insertMany(list);
				list = new ArrayList<>();
			}
		}
		transactions.insertMany(list);
		client.close();
		con.close();
	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
