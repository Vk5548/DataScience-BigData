package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

public class GenerateLOneOpt {

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
		System.out.println("\nmongoColTrans = "+ mongoColTrans + "\n mongoColL1 = "+ mongoColL1);
		/*
		 * 
		 * Extract single items from the transactions. Only single items that are present in at least minSup
		 *  transactions should survive.
		 * 
		 * Keep track of the transactions associated to each item using an array field named 'transactions'. 
		 * Also, use _ids such that
		 * 	they reflect the lexicographical order in which documents are processed.
		 * 
		 */


		AggregateIterable<Document> output = transactions.aggregate(Arrays.asList(Aggregates.unwind("$items"),
				Aggregates.group("$items", Accumulators.push("transactions", "$_id")),
				Aggregates.sort(Sorts.ascending("_id")),
				Aggregates.project(Projections.fields(Projections.computed(
	                    "count",
	                    Projections.computed("$size", "$transactions")),
						Projections.fields(Projections.computed("items", Projections.computed("pos_0", "$_id"))),
						Projections.include("transactions"),
						Projections.excludeId())
						),
				Aggregates.match(Filters.gte("count", minSup))
				));
		int count = 0;
		ArrayList<Document> ls = new ArrayList<>();
		int i =0;
		for(Document doc : output) {
			doc.append("_id", i++);
			count++;
			ls.add(doc);
			if(count % 1000 == 0) {
				l1.insertMany(ls);
				ls = new ArrayList<>();
			}
		}
		if(!ls.isEmpty()) {
			l1.insertMany(ls);
		}
		
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
