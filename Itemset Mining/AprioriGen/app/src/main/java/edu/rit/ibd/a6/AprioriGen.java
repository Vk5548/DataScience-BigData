package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.*;

import org.bson.Document;

import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class AprioriGen {
	private static HashSet<HashSet<Integer>> setLk = new HashSet<>();
	private static int kMinusOne = 0;

	private static Set<Set<Integer>> aprioriJoin(ArrayList<ArrayList<Integer>> lKMinusOne) {
		Set<Set<Integer>> setCk = new HashSet<>();
		for (ArrayList<Integer> p : lKMinusOne) {
			for (ArrayList<Integer> q : lKMinusOne) {
				HashSet<Integer> singleSetLk = new HashSet<>();
				boolean flag = true;
				for (int i = 0; i < kMinusOne - 1; i++) {
					int singleP = p.get(i);
					int singleQ = q.get(i);
					if (singleP == singleQ) {
						singleSetLk.add(singleP);
						continue;
					}
					flag = false;
				}
				if (flag) {
					int singleP = p.get(kMinusOne - 1);
					int singleQ = q.get(kMinusOne - 1);
					if (singleP < singleQ) {
						singleSetLk.add(singleP);
						singleSetLk.add(singleQ);
						setCk.add(singleSetLk);
					}
				}
			}
		}
		return setCk;
	}

	

	private static void insertIntoCk(MongoCollection<Document> ck, Set<Set<Integer>> current) {
		java.util.Iterator<Set<Integer>> hs = current.iterator();
		ArrayList<Document> al = new ArrayList<>();
		int count = 0;
		final int maxCount = 700;
		while (hs.hasNext()) {
			Set<Integer> single = hs.next();
			Object[] arr = single.toArray();
			Arrays.sort(arr);
			Document d = new Document().append("count", 0);
			Document sub = new Document();
			for (int i = 0; i <= kMinusOne; i++) {
				sub.append("pos_" + i, (Integer) arr[i]);
			}
			d.append("items", sub);
			al.add(d);
			if (count % maxCount == 0) {
				ck.insertMany(al);
				al = new ArrayList<>();
			}
		}
		// Cannot insert empty list
		if (!al.isEmpty()) {
			ck.insertMany(al);
			al = new ArrayList<>();
		}
	}

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColLKMinusOne = args[2];
		final String mongoColCK = args[3];

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> lKMinusOne = db.getCollection(mongoColLKMinusOne);
		MongoCollection<Document> ck = db.getCollection(mongoColCK);

		// TODO Your code here!
		ArrayList<ArrayList<Integer>> listLkMinusOne = new ArrayList<>();
		MongoCursor<Document> iterLkMinusOne = lKMinusOne.find().batchSize(5000).iterator();
		Set<Integer> ss = new HashSet<>();
		while (iterLkMinusOne.hasNext()) {
			HashSet<Integer> singleLk = new HashSet<>();
			ArrayList<Integer> set = new ArrayList<>();
			Document dLk = iterLkMinusOne.next();
			Document items = (Document) dLk.get("items");
			kMinusOne = items.entrySet().size();
			for (int i = 0; i < kMinusOne; i++) {
				ss.add(items.getInteger("pos_" + i));
				set.add(items.getInteger("pos_" + i));
				singleLk.add(items.getInteger("pos_" + i));
			}
			setLk.add(singleLk);
			listLkMinusOne.add(set);
		}

		// Skip prune step for K -1 = 1
		if (kMinusOne != 1) {
			Set<Set<Integer>> setCk = aprioriJoin(listLkMinusOne);
//			PRUNE STEP START
			Set<Set<Integer>> result = new HashSet<>();
			for (Set<Integer> set : setCk) {
				boolean flag = true;
				for (Set<Integer> subset : Sets.combinations(set, kMinusOne)) {
					if (setLk.contains(subset)) {
						continue;
					}
					flag = false;
					break;
				}
				if (flag) {
					result.add(set);
				}
			}
			//PRUNE STEP END
			insertIntoCk(ck, result);
		} else {
			Set<Set<Integer>> result1 = Sets.combinations(ss, kMinusOne + 1);
			insertIntoCk(ck, result1);
		}
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
