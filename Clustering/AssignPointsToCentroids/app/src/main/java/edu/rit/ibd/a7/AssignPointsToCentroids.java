package edu.rit.ibd.a7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.bson.Document;
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
import com.mongodb.client.model.Sorts;

import ch.obermuhlner.math.big.BigDecimalMath;




public class AssignPointsToCentroids {
	public enum Distance {Manhattan, Euclidean};
	public enum Mean {Arithmetic, Geometric};
	private static int size;
	private static final MathContext MC = MathContext.DECIMAL128;

	//get the distance
	private static BigDecimal getDistance(Decimal128[] point_dimensions, Decimal128[] centroid_dimensions, Distance type) {
		BigDecimal total_distance = BigDecimal.ZERO;
		for(int i =0; i< size; i++) {
			BigDecimal diff = BigDecimal.ZERO;
			BigDecimal p = point_dimensions[i].bigDecimalValue();
			BigDecimal q = centroid_dimensions[i].bigDecimalValue();
			if(type == Distance.Manhattan) {
				diff = p.subtract(q, MC).abs(MC);			
			}else {
				diff = q.subtract(p, MC).pow(2,MC);				
			}
			total_distance = total_distance.add(diff, MC);
		}
		if (type == Distance.Euclidean) {
			total_distance = total_distance.sqrt(MC);	
		}
		return total_distance;
	}
	
	//get dimensions for a point given a document
	private static Decimal128[] getDimensions(Document doc) {
		Decimal128[] result = new Decimal128[size];
		for(int i =0; i < size; i++) {
			result[i] = (Decimal128) doc.get("dim_"+i);
		}
		return result;
	}
	
	
	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final Distance distance = Distance.valueOf(args[3]);
		final Mean mean = Mean.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> collection = db.getCollection(mongoCol);
//		System.out.println("\nmongoCol => "+ mongoCol + "\ndistance => "+ distance + "\nmean => "+ mean); 
		// TODO Your code here!
		
		Bson filter = Filters.regex("_id","p_*"); //to get the total number of dimensions
		FindIterable<Document> doc =  collection.find(filter).limit(1);
		Document current_doc = doc.first();
		//get all the information
		
		Document current_point = (Document) current_doc.get("point");
		size = current_point.size(); //number of dimensions
		
		//number of centroids
		AggregateIterable<Document> iter = collection.aggregate(Arrays.asList(
				Aggregates.match(Filters.regex("_id", "c_*")),
				Aggregates.group(null, Accumulators.sum("totalCentroids", 1))
				));
		int totalCentroids = iter.first().getInteger("totalCentroids");
		
		//centroids
		List<Document> list = new ArrayList<>();
		list.add(new Document().append("$match" ,new Document().append("_id", new Document().append("$regex", "c_*"))));
		list.add(new Document().append("$sort", new Document().append("_id", 1)));
		MongoCursor<Document> cursor = collection.aggregate(list).batchSize(5000).iterator();
		
		Document[] centroid = new Document[totalCentroids];
		while(cursor.hasNext()) {
			Document c = cursor.next();
			Integer id = Integer.parseInt(c.get("_id").toString().substring(2));
			
			centroid[id] = (Document)c.get("centroid");
		}
		cursor.close();
		
		//hashmap for storing id, SSe 
		HashMap<String, BigDecimal> hm = new HashMap<>();
		// another hashMap for storing sum of separate dimensions //last value will be total number of points
		HashMap<String, BigDecimal[]> hmPointsD = new HashMap<>();
		
		//points
		List<Document> listPoint = new ArrayList<>();
		listPoint.add(new Document().append("$match", new Document().append("_id", new Document().append("$regex", "p_*"))));
		MongoCursor<Document> cursorPoint = collection.aggregate(listPoint).batchSize(5000).iterator();
		
		//assigning points
		while(cursorPoint.hasNext()) {
			Document point = cursorPoint.next();
			Document dimensionP =  (Document) point.get("point"); 
			
			Decimal128[] pointD = getDimensions(dimensionP); //point dimensions
			int minCentroidId = -1;
			BigDecimal minimumDistance = new BigDecimal(Integer.MAX_VALUE);
			for(int i =0; i< totalCentroids; i++ ) {
				//dimensions of centroid
				Decimal128[] centroidD = getDimensions(centroid[i]);
				
				BigDecimal dist = getDistance(pointD, centroidD, distance);
				BigDecimal diff = minimumDistance.subtract(dist, MC);
				if(BigDecimal.ZERO.max(diff) == diff) {
					minimumDistance = dist;
					minCentroidId = i;
				}
			}
			
			//updating closest centroid
			collection.updateOne(new Document().append("_id", point.getString("_id")), 
					new Document().append("$set", new Document().append("label", "c_"+ minCentroidId)));
			
			//putting the values in the centroid hm
			BigDecimal val = hm.get("c_"+ minCentroidId);
			if(val == null) {
				hm.put("c_"+ minCentroidId, minimumDistance.pow(2, MC));
			}else {
				val = val.add( minimumDistance.pow(2, MC), MC);
				hm.put("c_"+ minCentroidId, val);
			}
			
			//putting the values in the dimension hashmap
			BigDecimal[] arr = hmPointsD.get("c_"+ minCentroidId);
			if(arr == null) {
				arr = new BigDecimal[size + 1];
				for(int i = 0; i< size; i++) {
					if(mean == Mean.Arithmetic) {
						arr[i] =pointD[i].bigDecimalValue();
					}else {
						arr[i] = BigDecimalMath.log(pointD[i].bigDecimalValue(), MC);
					}
				}
				arr[size] = BigDecimal.ONE;
			}else {
				for(int i = 0; i< pointD.length; i++) {
					BigDecimal dim = BigDecimal.ZERO;
					if(mean == Mean.Geometric) {
						 dim = BigDecimalMath.log(pointD[i].bigDecimalValue(), MC);
					}else {
						dim = pointD[i].bigDecimalValue();
					}
					arr[i] = arr[i].add(dim, MC);
				}
				arr[size] = arr[size].add(BigDecimal.ONE, MC);
			}
			hmPointsD.put("c_"+ minCentroidId, arr);
		}
		cursorPoint.close();
		
		
		
		//inserting new ci
		ArrayList<Document> newCI = new ArrayList<>();
		int count = 0, batch = 20;
		for(String id: hmPointsD.keySet()) {
			Document d = new Document().append("_id", "new_"+id);
			Document dims = new Document();
			BigDecimal[] arr = hmPointsD.get(id);
			for(int i =0; i< size; i++) {
				arr[i] = arr[i].multiply(arr[size].pow(-1, MC), MC); //dividing by total number of points
				if(mean == Mean.Geometric) {
					arr[i] = BigDecimalMath.exp(arr[i], MC);
				}
				dims.append("dim_"+i, new Decimal128(arr[i]));
			}
			d.append("centroid", dims);
			newCI.add(d);
			count++;
			if(count % batch == 0) {
				collection.insertMany(newCI);
				newCI = new ArrayList<>();
			}
		}
		if(!newCI.isEmpty()) {
			collection.insertMany(newCI);
			newCI = new ArrayList<>();
		}
		
		//inserting values in centroid document
//		System.out.println("hm >>" + hm.toString());
		for(String key: hm.keySet()) {
			collection.updateOne(new Document().append("_id", key), 
					new Document().append("$set", new Document().append("sse", hm.get(key))));
			collection.updateOne(new Document().append("_id", key), 
					new Document().append("$set", new Document().append("reinitialize", false)));
		}
		// inserting totalPoints in centroid document
		AggregateIterable<Document> findPoints = collection.aggregate(Arrays.asList(
				Aggregates.match(Filters.regex("_id", "p_*")),
				Aggregates.group("$label", Accumulators.sum("totalPoints", 1))
				));
		
		for(Document d: findPoints) {
			String id = d.getString("_id");
			Integer totalPoints = d.getInteger("totalPoints");
			collection.updateOne(new Document().append("_id", id), 
					new Document().append("$set", new Document().append("total_points", totalPoints)));
		}
		
		// inserting reinitialize = true for rest of the points
		List<Document> ls = new ArrayList<>();
		ls.add(new Document().append("$match", new Document().append("_id", new Document().append("$regex", "^c_*")).append("sse", 
				new Document().append("$exists", 0))));
		
		MongoCursor<Document> nonInitCentroids = collection.aggregate(ls).iterator();
		while(nonInitCentroids.hasNext()) {
			Document d = nonInitCentroids.next();
			String id = d.getString("_id");
			collection.updateOne(new Document().append("_id", id), 
					new Document().append("$set", new Document().append("reinitialize", true)));

		}
		nonInitCentroids.close();
		
		
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
