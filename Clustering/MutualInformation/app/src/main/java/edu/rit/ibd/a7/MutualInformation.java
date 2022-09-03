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
import com.mongodb.client.model.Sorts;

import ch.obermuhlner.math.big.BigDecimalMath;


public class MutualInformation {
	private static final MathContext MC = MathContext.DECIMAL128;
	
	private static BigDecimal getH(ArrayList<Decimal128> list, Decimal128 n, int length) {
		BigDecimal sum = BigDecimal.ZERO;
		BigDecimal totalPoints = n.bigDecimalValue();
		for(int i =0; i < length; i++) {
			 BigDecimal val = list.get(i).bigDecimalValue();
			 if(val == BigDecimal.ZERO) {
				 continue;
			 }
			 val = val.divide(totalPoints, MC);
			 if (val.signum() > 0) {
				 val = val.multiply(BigDecimalMath.log(val, MC), MC);
				 sum = sum.add(val, MC);
				}
			 
		 }
		sum = sum.multiply(new BigDecimal(-1), MC);
		sum =  sum.setScale(sum.scale() + MathContext.DECIMAL128.getPrecision() - sum.precision(), MathContext.DECIMAL128.getRoundingMode());

		return sum;
		
	}

	
	
	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final int R = Integer.valueOf(args[3]);
		final int C = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> collection = db.getCollection(mongoCol);
		//creating index
		collection.createIndex(new Document().append("label_u", 1));
		collection.createIndex(new Document().append("label_v", 1));
//		System.out.println("\nmongoCol => "+ mongoCol + "\nR => " + R +"\nC => " + C );
		// TODO Your code here!
		Document mi = new Document().append("_id", "mi_info");
		// calculating a
		AggregateIterable<Document> output_u = collection.aggregate(Arrays.asList(
				Aggregates.group("$label_u", Accumulators.sum("count_u", 1)),
				Aggregates.sort(Sorts.ascending("_id"))
				));
		
		
		ArrayList<Decimal128> ls = new ArrayList<>();
		Decimal128[] arr = new Decimal128[R];
		for(Document a: output_u) {
			Integer id = a.getInteger("_id");
			Integer count_u = 0;
			if(id != null) {
				count_u = a.getInteger("count_u");
				arr[id] = new Decimal128(count_u);
			}
		}
		for(int i =0; i< R; i++){
			if(arr[i] == null) {
				ls.add(new Decimal128(0));
			}else {
				ls.add(arr[i]);
			}
		}
		mi.append("a", ls);
		collection.insertOne(mi);
		ls = new ArrayList<>();
		
		//calculating b
		AggregateIterable<Document> output_v = collection.aggregate(Arrays.asList(
				Aggregates.group("$label_v", Accumulators.sum("count_v", 1)),
				Aggregates.sort(Sorts.ascending("_id"))
				));
		arr = new Decimal128[C];
		for(Document a: output_v) {
			Integer id = a.getInteger("_id");
			Integer count_v = 0;
			if(id != null) {
				count_v = a.getInteger("count_v");
				arr[id] = new Decimal128(count_v);
			}
		}
		for(int i =0; i< C; i++){
			if(arr[i] == null) {
				ls.add(new Decimal128(0));
			}else {
				ls.add(arr[i]);
			}
		}
		
//		System.out.println("aarr[o] = " + arr[0]);
		collection.updateOne(new Document().append("_id", "mi_info"), new Document().append("$set", new Document().append("b", ls)));
		ls = new ArrayList<>();
		
		//CALCULATING N
				AggregateIterable<Document> output = collection.aggregate(Arrays.asList(Aggregates.match(Filters.regex("_id", "p_*")),
						Aggregates.group(null, Accumulators.sum("total_points", 1))
						));
//				System.out.println("output.first() => " + output.first().toString());
				int total_points = (int) output.first().get("total_points");
				
				Decimal128 n = new Decimal128(total_points);
				collection.updateOne(new Document().append("_id", "mi_info"), new Document().append("$set", new Document().append("n", n)));
				
				
				//CALCULATING H(U)
				Bson h_u = Filters.eq("_id", "mi_info");
				 FindIterable<Document> doc =  collection.find(h_u);
				 Document single = doc.first();
				 
				 ArrayList<Decimal128> a_list = (ArrayList<Decimal128>) single.getList("a", Decimal128.class);
				 BigDecimal val = getH(a_list, n, R);
				
				 collection.updateOne(new Document().append("_id", "mi_info"), new Document().append("$set", new Document().append("hu", new Decimal128(val))));
				//CALCULATING H(V)
				 
				 
				 
				 ArrayList<Decimal128> b_list = (ArrayList<Decimal128>) single.getList("b", Decimal128.class);
				 BigDecimal valV = getH(b_list, n, C);
				 
				 collection.updateOne(new Document().append("_id", "mi_info"), new Document().append("$set", new Document().append("hv", new Decimal128(valV))));

		//CALCULATING C
		
		BigDecimal miSum = BigDecimal.ZERO;
		for(int i =0; i< R; i++) {
			for(int j =0; j < C; j++) {
				Bson filter = Filters.and(Filters.eq("label_u", i),
						Filters.eq("label_v", j));
				Long count = collection.countDocuments(filter);
				ls.add(new Decimal128(count));
				//mi
				BigDecimal totalPoints = n.bigDecimalValue();
				BigDecimal valueMI = new BigDecimal(count);
				BigDecimal out = valueMI.multiply( totalPoints.pow(-1, MC), MC);
				BigDecimal pU = a_list.get(i).bigDecimalValue().multiply( b_list.get(j).bigDecimalValue(), MC);
//				BigDecimal pV = b_list.get(j).bigDecimalValue().multiply( totalPoints.pow(-1, MC), MC);
//				pU = pU.multiply(pV, MC);
				if (pU.signum() != 0) {      // x/0
		            if (valueMI.signum() != 0)   { // 0/0
		            	if(!pU.equals(BigDecimal.ZERO)) {
							BigDecimal another = valueMI.multiply( pU.pow(-1, MC), MC);
							another = another.multiply(totalPoints, MC);
							if (another.signum() > 0) {
								 another = BigDecimalMath.log(another, MC);
								 out = out.multiply(another, MC);
								 miSum = miSum.add(out);
								}
							miSum =  miSum.setScale(miSum.scale() + MathContext.DECIMAL128.getPrecision() - miSum.precision(), MathContext.DECIMAL128.getRoundingMode());

						}
		            } 
		               
		        }
				
				
				
			}
		}
		collection.updateOne(new Document().append("_id", "mi_info"), new Document().append("$set", new Document().append("c", ls)));
		collection.updateOne(new Document().append("_id", "mi_info"), new Document().append("$set", new Document().append("mi", new Decimal128(miSum))));

		ls = new ArrayList<>();
		
		//storing log
		BigDecimal[] log_values = new BigDecimal[total_points];
		log_values[0] = BigDecimal.ZERO;
		log_values[1] = BigDecimal.ZERO;
		for(int i = 2; i< log_values.length; i++) {
			log_values[i] = BigDecimalMath.log(new BigDecimal(i), MC).add(log_values[i - 1], MC);
//			log_values[i] = new BigDecimal(i).multiply(log_values[i - 1], MC);
		}
		//calculating emi
		BigDecimal emi = BigDecimal.ZERO;
		for(int i =0; i< R; i++) {
			for(int j =0; j< C; j++) {
				BigDecimal m = n.bigDecimalValue();
				BigDecimal aI = a_list.get(i).bigDecimalValue();
				BigDecimal bj = b_list.get(j).bigDecimalValue();
				BigDecimal lowerLimit = aI.add(bj, MC).subtract(m, MC);
				lowerLimit = lowerLimit.max(BigDecimal.ZERO);
				BigDecimal upperLimit = aI.min(bj);
				for(int k = lowerLimit.intValue(); k < upperLimit.intValue(); k++) {
					BigDecimal out = new BigDecimal(k).multiply(m.pow(-1, MC), MC); //first
					BigDecimal log = new BigDecimal(k).multiply(m, MC);
					BigDecimal denomenator = aI.multiply(bj, MC);
					log = log.multiply(denomenator.pow(-1, MC), MC);
					if(log.signum() > 0) {
						log = BigDecimalMath.log(log, MC); //second
						int aInt = aI.intValue();
						int bInt = bj.intValue();
						int mInt = m.intValue() - 1; //total_points
						
						BigDecimal num = log_values[aInt].add(log_values[bInt], MC);
						num = num.add(log_values[mInt - aInt], MC);
						num = num.add(log_values[mInt - bInt], MC);
						BigDecimal den = log_values[mInt].add(log_values[k], MC).add(log_values[aInt - k], MC);
						den = den.add(log_values[bInt - k], MC);
						den = den.add(log_values[mInt - aInt - bInt + (k)], MC);
						num = num.subtract(den, MC); //third
						num = BigDecimalMath.exp(num, MC);
//						BigDecimal final 
						out = out.multiply(log, MC).multiply(num, MC);
						emi = emi.add(out, MC);
					}
					
					
				}
			}

			System.out.println("i "+i+"   emi ===> " + emi);
		}
		System.out.println("emi ===> " + emi);
		collection.updateOne(new Document().append("_id", "mi_info"), new Document().append("$set", new Document().append("emi", new Decimal128(emi))));

		/*
		 * 
		 * Every point will have two labels: label_u is an integer indicating the cluster of the point in assignment U; similarly, label_v is the
		 * 	cluster (integer) of the point in assignment V. |U|=R and |V|=C.
		 * 
		 * You need to compute fields a, b and c. Field a is an array of size R in which each position a.i indicates the total number of points 
		 * 	assigned to cluster i in U. Similarly, b is an array of size C; each position b.j is the total number of points assigned to cluster j
		 * 	in V. Field c is an array storing the contingency matrix. In the contingency matrix, [i, j] is the number of points that are assigned
		 * 	to both i in U and j in V. Since MongoDB does not allow us to store matrices, we are going to store the matrix as a single array. 
		 * 	Note that the contingency matrix has R rows and C columns. Cell [i, j] is position p=i*C+j in the array c. Furthermore, position p in
		 * 	c corresponds to the following cell: [(p - p%C)/C, p%C].
		 * 
		 * Using the previous fields, you need to compute H(U), H(V), MI and E(MI) as defined in the slides. You must store them in the following
		 * 	fields: hu, hv, mi and emi, respectively.
		 * 
		 * To compute the factorial part of E(MI), you should use log factorial. The following library is a good resource to work with BigDecimal
		 * 	in Java: https://github.com/eobermuhlner/big-math
		 * 
		 * All these fields must be stored in a single document with _id=mi_info.
		 * 
		 */

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
