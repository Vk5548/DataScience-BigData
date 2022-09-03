package edu.rit.ibd.a7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

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
import com.mongodb.client.model.Sorts;

//myImports
import ch.obermuhlner.math.big.BigDecimalMath;




public class SilhouetteCoefficient {
	public enum Distance {Manhattan, Euclidean};
	public enum Mean {Arithmetic, Geometric};
	
	private static final MathContext MC = MathContext.DECIMAL128;

	//get the distance between points
	private static BigDecimal getDistance(Decimal128[] current_dimensions, Decimal128[] dimensions, int size, Distance type) {
		BigDecimal total_distance = BigDecimal.ZERO;
		for(int i =0; i< size; i++) {
			BigDecimal diff = BigDecimal.ZERO;
			BigDecimal p = current_dimensions[i].bigDecimalValue();
			BigDecimal q = dimensions[i].bigDecimalValue();
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
	
	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final String pointId = args[3];
		final Distance distance = Distance.valueOf(args[4]);
		final Mean mean = Mean.valueOf(args[5]);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> collection = db.getCollection(mongoCol);
		
//		System.out.println("\nmongoCol --> " + mongoCol + "\n pointId --> " +
//		pointId + "\n distance " + distance + "\n mean -->" + mean);
		// TODO Your code here!

		
		// calculate a
		Bson current = Filters.eq("_id", pointId);
		FindIterable<Document> doc =  collection.find(current);
		Document current_doc = doc.first();
		//get all the information
		String current_centroid = current_doc.getString("label");
		Document current_point = (Document) current_doc.get("point");
		Integer size = current_point.size();
		Decimal128[] current_dimensions = new Decimal128[size];
//		ArrayList<Decimal128> current_dimensions = new ArrayList<>();
		for(int i =0; i < size; i++) {
			current_dimensions[i] = (Decimal128) current_point.get("dim_"+i);
		}
//		System.out.println("\n current_centroid = "+ current_point.toString() + "\nsize === " + size);
		
		Bson all_points = Filters.eq("label", current_centroid);
		long countMatchingDocuments = collection.countDocuments(all_points); 
		FindIterable<Document> all_points_doc =  collection.find(all_points);
		
		BigDecimal aMean = BigDecimal.ZERO;
		BigDecimal gMean = BigDecimal.ZERO;

		for(Document d : all_points_doc) {
//			System.out.println("\n d = "+ d.toString());
			Decimal128[] dimensions = new Decimal128[size];
			Document other = (Document) d.get("point");
			for(int i =0; i < size; i++) {
				dimensions[i] = (Decimal128) other.get("dim_"+i);
			}
			BigDecimal val = getDistance(current_dimensions, dimensions, size, distance);
			if(mean == Mean.Arithmetic) {
				aMean = aMean.add(val, MC);
			}else {
				if (val.signum() > 0) {
					val = BigDecimalMath.log(val, MC);
					gMean = gMean.add(val, MC);
				}
			}
		}
		Document a = new Document();
		BigDecimal count = new BigDecimal(countMatchingDocuments -1);
		if(mean == Mean.Arithmetic) {
			aMean = aMean.divide(count, MC);
			a.append("a", aMean);
		}else {
			gMean = gMean.divide(count, MC);
			gMean = BigDecimalMath.exp(gMean, MC);
			a.append("a", gMean);
		}
		
		collection.updateOne(new Document().append("_id", pointId), new Document().append("$set", a));
//		 find the number of centroids
		AggregateIterable<Document> output = collection.aggregate(Arrays.asList(Aggregates.match(Filters.regex("_id", "c_*")),
				Aggregates.group(null, Accumulators.sum("total_centroids", 1))
				));
		int total_centroids = (int) output.first().get("total_centroids");
		
//		System.out.println("\n total_centroids => "+ total_centroids);

		for(int i =0; i< total_centroids; i++) {
			if(current_centroid.equals("c_"+i)) {
				continue;
			}
			Bson filter = Filters.eq("label", "c_"+i);
			int total_points = (int) collection.countDocuments(filter);
			
			if(total_points != 0) {
				BigDecimal dist = BigDecimal.ZERO;
				FindIterable<Document> points = collection.find(filter);
				
				for(Document point : points) {
					Decimal128[] dimensions = new Decimal128[size];
					Document other = (Document) point.get("point");
					for(int j =0; j < size; j++) {
						dimensions[j] = (Decimal128) other.get("dim_"+j);
					}
					BigDecimal val = getDistance(current_dimensions, dimensions, size, distance);
					if(mean == Mean.Arithmetic) {
						dist = dist.add(val, MC);
					}else {
						if (val.signum() > 0) {
							val = BigDecimalMath.log(val, MC);
							dist = dist.add(val, MC);
						}
					}
				}
				Document other_dist = new Document();
				BigDecimal count_other = new BigDecimal(total_points);
				if(mean == Mean.Arithmetic) {
					dist = dist.divide(count_other, MC);
					other_dist.append("d_"+i, dist);
				}else {
					dist = dist.divide(count, MC);
					dist = BigDecimalMath.exp(dist, MC);
					other_dist.append("d_"+i, dist);
				}
				
				collection.updateOne(new Document().append("_id", pointId), new Document().append("$set", other_dist));

			}
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
