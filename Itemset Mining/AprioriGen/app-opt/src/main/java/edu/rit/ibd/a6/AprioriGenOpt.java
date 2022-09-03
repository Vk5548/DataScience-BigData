package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class AprioriGenOpt {
	private static HashSet<HashSet<Integer>> setLk = new HashSet<>();
	private static int kMinusOne = 0;
//	private static HashMap<Integer, ArrayList<ArrayList<Integer>>> hm = new HashMap<>(); //initial transactions
	private static HashMap<Set<Integer>, List<Integer>> transHashMap = new HashMap<>(); //initial transactions
	private static HashMap<Set<Integer>, Set<Integer>> transHashMap1 = new HashMap<>();//after join step transactions

	/*
	 * intersectTransactions:
	 * result : Resultant set which contains set of new Lk
	 * minSup
	 */
	private static HashMap<Set<Integer>, Set<Integer>> intersectTransactions(Set<Set<Integer>> result, int minSup) {
//		System.out.println("result method "+ result);
		Iterator<Set<Integer>> c = result.iterator();

		HashMap<Set<Integer>, Set<Integer>> intersectedTxnMap = new HashMap<>();
		while (c.hasNext()) { // iterating over result
			Set<Integer> x = c.next(); //single set of items after join step: size = 2
			Set<Integer> xx1 = new HashSet<>();
			Object[] xa1 = x.toArray(); //
			xx1.add((Integer) xa1[0]); // getting the first item
			Set<Integer> xx2 = new HashSet<>();
			xx2.add((Integer) xa1[1]); // getting the second item
			ArrayList<Integer> tL1 = (ArrayList<Integer>) transHashMap.get(xx1); // getting the respective transactions
			ArrayList<Integer> tL2 = (ArrayList<Integer>) transHashMap.get(xx2);
			Set<Integer> t1 = new HashSet<>();
			Set<Integer> t2 = new HashSet<>();
			//getting the transactions into set for intersection
			t1 = convertListToSet(tL1);
			t2 = convertListToSet(tL2);
//			for (Integer i : tL1) {
//				t1.add(i); 
//			}
//			for (Integer i : tL2) {
//				t2.add(i);
//			}
			Set<Integer> intersectedTrans = Sets.intersection(t1, t2); // intersected transactions

			if (intersectedTrans.size() >= minSup) {
				intersectedTxnMap.put(x, intersectedTrans);
			}

		}
		return intersectedTxnMap; // returning the current itemList and corresponding transactions
	}

	private static Set<Integer> convertListToSet(ArrayList<Integer> list) {
		Set<Integer> itemSet = new HashSet<>(); 
		for (Integer i : list) {
			itemSet.add(i);
		}
		return itemSet;

	}
//	private static Set<Set<Integer>> aprioriJoinTest(ArrayList<ArrayList<Integer>> lKMinusOne, int minSup){
//		int length = hm.size();
//		Set<Set<Integer>> setCk = new HashSet<>(); 
//		for(int i =0; i< length - 1; i++) {
//			for(int j = i+ 1; j < length ; j++) {
//				ArrayList<Integer> p = lKMinusOne.get(i);
//				ArrayList<Integer> q = lKMinusOne.get(j);
//				Set<Integer> singleSetLk = new HashSet<>();
//				boolean flag = true;
//				for (int l = 0; l < kMinusOne - 1; l++) {
//					int singleP = p.get(l);
//					int singleQ = q.get(l);
//					if (singleP == singleQ) {
//						singleSetLk.add(singleP);
//						continue;
//					}
//					flag = false;
//				}
//				if (flag) {
//					int singleP = p.get(kMinusOne - 1);
//					int singleQ = q.get(kMinusOne - 1);
//					if (singleP < singleQ) {
//						ArrayList<Integer> pTrans = (ArrayList<Integer>) transHashMap.get(convertListToSet(p));
//						ArrayList<Integer> qTrans = (ArrayList<Integer>) transHashMap.get(convertListToSet(q));
//						Set<Integer> pSetTrans = convertListToSet(pTrans);
//						Set<Integer> qSetTrans = convertListToSet(qTrans);
//
//						Set<Integer> intersected = Sets.intersection(pSetTrans, qSetTrans);
//						
//						if (intersected.size() >= minSup) {
//							singleSetLk.add(singleP);
//							singleSetLk.add(singleQ);
//							transHashMap1.put(singleSetLk, intersected);
//							setCk.add(singleSetLk);
//						}
//					}
//				}
//			}
//		}
//		return setCk;
//	}

	private static Set<Set<Integer>> aprioriJoin(ArrayList<ArrayList<Integer>> lKMinusOne, int minSup) {
		
		Set<Set<Integer>> setCk = new HashSet<>(); //contains updated next level items after joining
		for (ArrayList<Integer> p : lKMinusOne) {
			for (ArrayList<Integer> q : lKMinusOne) {
				Set<Integer> singleSetLk = new HashSet<>();
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
						ArrayList<Integer> pTrans = (ArrayList<Integer>) transHashMap.get(convertListToSet(p));
						ArrayList<Integer> qTrans = (ArrayList<Integer>) transHashMap.get(convertListToSet(q));
						Set<Integer> pSetTrans = convertListToSet(pTrans);
						Set<Integer> qSetTrans = convertListToSet(qTrans);

						Set<Integer> intersected = Sets.intersection(pSetTrans, qSetTrans);
						
						if (intersected.size() >= minSup) {
							singleSetLk.add(singleP);
							singleSetLk.add(singleQ);
							transHashMap1.put(singleSetLk, intersected);
							setCk.add(singleSetLk);
						}
					}
				}
			}
		}
		return setCk;
	}

	private static List<Integer> getTransactionsList(HashMap<Set<Integer>, Set<Integer>> transhm, Set<Integer> items) {
		List<Integer> transList = new ArrayList<>();
			Object[] transactionSet =  transhm.get(items).toArray();
			Arrays.sort(transactionSet);
			for(Object i : transactionSet) {
				transList.add(Integer.parseInt(i.toString()));
			}
		return transList;
	}
	
	private static void insertIntoLk(MongoCollection<Document> ck, Set<Set<Integer>> current,
			HashMap<Set<Integer>, Set<Integer>> transhm) {
		java.util.Iterator<Set<Integer>> hs = current.iterator();
		ArrayList<Document> al = new ArrayList<>();
		int count = 0;
		final int maxCount = 700;
		int _id =0;
		while (hs.hasNext()) {
			List<Integer> transList = new ArrayList<>();
			Set<Integer> single = hs.next();
			Object[] arr = single.toArray();
			Arrays.sort(arr);
			Document d = new Document();
			Document sub = new Document();
			for (int i = 0; i <= kMinusOne; i++) {
				sub.append("pos_" + i, (Integer) arr[i]);
			}
			d.append("items", sub);
			
			if(transhm.containsKey(single)) {
				transList = getTransactionsList(transhm, single);
				d.append("transactions", transList);
				d.append("count", transList.size());
				d.append("_id", _id++);
				al.add(d);
				if (count % maxCount == 0) {
					ck.insertMany(al);
					al = new ArrayList<>();
				}
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
		final String mongoColLK = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> lKMinusOne = db.getCollection(mongoColLKMinusOne);
		MongoCollection<Document> lk = db.getCollection(mongoColLK);
		System.out.println("\nmongoColLKMinusOne = " + mongoColLKMinusOne + "\n mongoColLK = " + mongoColLK);

		ArrayList<ArrayList<Integer>> listLkMinusOne = new ArrayList<>();
		MongoCursor<Document> iterLkMinusOne = lKMinusOne.find().batchSize(5000).iterator();
		Set<Integer> ss = new HashSet<>();
		
		while (iterLkMinusOne.hasNext()) {
			HashSet<Integer> singleLk = new HashSet<>();
			ArrayList<Integer> list = new ArrayList<>();
			Document dLk = iterLkMinusOne.next();
//			int _id =  dLk.getInteger("_id");
			Document items = (Document) dLk.get("items");
			List<Integer> transList = dLk.getList("transactions", Integer.class);
			kMinusOne = items.entrySet().size();
			for (int i = 0; i < kMinusOne; i++) {
				ss.add(items.getInteger("pos_" + i)); // just for when kMinusOne = 1
				list.add(items.getInteger("pos_" + i));
				singleLk.add(items.getInteger("pos_" + i));
			}
			transHashMap.put(singleLk, transList);
			setLk.add(singleLk);
			listLkMinusOne.add(list);
//			hm.put(_id,listLkMinusOne);
		}
		// TODO Your code here!
		if (kMinusOne != 1) {
			
			Set<Set<Integer>> setCk = aprioriJoin(listLkMinusOne, minSup);
//			Set<Set<Integer>> setCk = aprioriJoinTest(listLkMinusOne, minSup); // tried the method discussed in class. Didn't work for me
			insertIntoLk(lk, setCk, transHashMap1);
//			System.out.println("size " + transHashMap1.size());
			
		} else {
			Set<Set<Integer>> result1 = Sets.combinations(ss, kMinusOne + 1);

			HashMap<Set<Integer>, Set<Integer>> intersectedTxns = intersectTransactions(result1, minSup);

//			System.out.println("size opt = " + intersectedTxns.size());
			insertIntoLk(lk, result1, intersectedTxns);

			
			
		}

		/*
		 * 
		 * The documents include the transactions that contain them, so if a new
		 * document is added to CK, we can directly compute its transactions by
		 * performing the intersection. Having the actual transactions entails that we
		 * also know its number, so we can discard those that do not meet the minimum
		 * support. Items can be processed in ascending order.
		 * 
		 */

		// You must figure out the value of k - 1.

		// You can implement this "by hand" using Java, an aggregation query, or a mix.

		// Remember that there is a single join step. The prune step is not used
		// anymore.

		// Make sure the _ids of the documents are according to the lexicographical
		// order of the items.
		// You can start joining documents
		// whose _ids are strictly greater than the current document. Also, the first
		// time a pair of documents do not join,
		// we can safely stop.

		// Both documents contain the arrays of transactions lexicographically sorted.
		// The new document will have the
		// intersection of both sets
		// of transactions.

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
