package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.bson.Document;
import org.bson.BsonArray;

import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

public class GenerateLK {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColCK = args[3];
		final String mongoColLK = args[4];
		final int minSup = Integer.valueOf(args[5]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> ck = db.getCollection(mongoColCK);
		MongoCollection<Document> lk = db.getCollection(mongoColLK);
		
		// TODO Your code here!
//		System.out.println("\n mongoColTrans --> "+ mongoColTrans + "\n mongoColCK ---> " + mongoColCK + "\n mongoColLK --> "+
//				mongoColLK  +" \n minSup ---> "+ minSup );
//		mongoColTrans --> Transactions_1
//		 mongoColCK ---> C2_1_2
//		 mongoColLK --> L2_1_2 
//		 minSup ---> 2

		transactions.createIndex(new Document().append("items", 1));
		
		MongoCursor<Document> iterCk = ck.find().batchSize(5000).iterator();
		ArrayList<Document> ls = new ArrayList<>();
		int count = 0;
		while(iterCk.hasNext()) {
			Document dCk = iterCk.next();
			Document docC = (Document) dCk.get("items");
			List<Integer> items = new ArrayList<>();
			int i =0;
			for(Entry<String, Object> currentPos : docC.entrySet()) {
				items.add((Integer) currentPos.getValue());
			}
			
			Bson filters =  Filters.all("items", items);
			items = new ArrayList<>();
			Long currentSup =  transactions.countDocuments(filters);
			
			if(currentSup >= minSup) {
//				System.out.println("current sup --> "+ currentSup);
				dCk.append("count", currentSup.intValue());
				ls.add(dCk);
				count++;
//				lk.insertOne(dCk);
				if(count % 300 == 0) {
					lk.insertMany(ls);
					ls = new ArrayList<>();
				}
				
			}
			 
			
		}
		lk.insertMany(ls);
		ls = new ArrayList<>();
		/*
		 * 
		 * For each transaction t, check whether the items of a document c in ck are contained in the items of t. If so, increment by one the count of c.
		 * 
		 * All the documents in ck that meet the minimum support will be copied to lk.
		 * 
		 * You can use $inc to update the count of a document.
		 * 
		 * Alternatively, you can also copy all documents in ck to lk first and, then, perform the previous computations.
		 * 
		 */
		
		// You must figure out the value of k.
		
		// For each document in Ck, check the items are present in the transactions at least minSup times.
		
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		
		// TODO End of your code here!
		
		client.close();
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
