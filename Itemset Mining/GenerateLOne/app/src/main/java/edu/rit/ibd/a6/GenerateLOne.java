package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

public class GenerateLOne{

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColL1 = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> l1 = db.getCollection(mongoColL1);
		
		// TODO Your code here!
		AggregateIterable<Document> output = transactions.aggregate(Arrays.asList(Aggregates.unwind("$items"),
				Aggregates.group("$items", Accumulators.sum("count", 1)),
				Aggregates.match(Filters.gte("count", minSup)),
				Aggregates.project(Projections.fields(Projections.computed("items", Projections.computed("pos_0", "$_id")),
						Projections.excludeId(),
		                Projections.include("count")) 
						)
				));
		int count = 0;
		ArrayList<Document> ls = new ArrayList<>();
		for(Document doc : output) {
			count++;
			ls.add(doc);
			if(count % 1000 == 0) {
				l1.insertMany(ls);
				ls = new ArrayList<>();
			}
		}
		l1.insertMany(ls);
		/*
		 * 
		 * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
		 * 
		 * You need to compose the new documents to be inserted in the L1 collection as {_id: {pos_0:iid}, count:z}.
		 * 
		 */
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		// Be mindful of main memory and use batchSize when you request documents from MongoDB.
		// TODO End of your code!
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
