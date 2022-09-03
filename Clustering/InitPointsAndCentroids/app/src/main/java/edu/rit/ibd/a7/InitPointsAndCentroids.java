package edu.rit.ibd.a7;

import static com.mongodb.client.model.Filters.ne;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

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
import com.mysql.jdbc.ResultSetMetaData;

public class InitPointsAndCentroids {
	public enum Scaling {None, MinMax, Mean, ZScore}
	private static final MathContext MC = MathContext.DECIMAL128;
	private static Document calculateLimit(ArrayList<Decimal128> dim) throws SQLException {
		Document dim_i = new Document();
		BigDecimal min = new BigDecimal(Integer.MAX_VALUE);
		BigDecimal max = new BigDecimal(Integer.MIN_VALUE);
		
		
		BigDecimal sum = BigDecimal.ZERO;
		
		for(Decimal128 val :dim) {
			BigDecimal value = val.bigDecimalValue();

			min = min.min(value);
			max = max.max(value);	
			sum = sum.add(value, MC);
			}
		BigDecimal n = new BigDecimal(dim.size());
		BigDecimal mean = sum.divide(n, MC);
		BigDecimal stdSum = BigDecimal.ZERO;
		for(Decimal128 val :dim) { 
			BigDecimal value = val.bigDecimalValue();
			BigDecimal diff = value.subtract(mean, MC);
			BigDecimal pow = diff.pow(2, MC);
			stdSum = stdSum.add(pow, MC);
		}
		BigDecimal divideStdSum = stdSum.divide(n, MC);
		BigDecimal std = divideStdSum.sqrt(MC);
		dim_i.append("min", min).append("max", max).append("mean", mean).append("std", std);;
		
		
		return dim_i;
		
	}
	
	private static Decimal128 getScaledValue(Scaling scaling, Decimal128 dim, 
			Decimal128 min, Decimal128 max, Decimal128 mean, Decimal128 std) {
		BigDecimal result = min.bigDecimalValue();
		BigDecimal minimum = min.bigDecimalValue();
		BigDecimal maximum = max.bigDecimalValue();
		BigDecimal avg = mean.bigDecimalValue();
		BigDecimal stdDeviation = std.bigDecimalValue();
		BigDecimal value = dim.bigDecimalValue();
		if(scaling == Scaling.MinMax) {
			BigDecimal numerator = value.subtract(minimum, MC);
			BigDecimal denomenator = maximum.subtract(minimum, MC);
			result = numerator.divide(denomenator, MC);
		}else if(scaling == Scaling.Mean) {
			BigDecimal numerator = value.subtract(avg, MC);
			BigDecimal denomenator = maximum.subtract(minimum, MC);
			result = numerator.divide(denomenator, MC);
		}else if(scaling == Scaling.ZScore) {
			BigDecimal numerator = value.subtract(avg, MC);
			result = numerator.divide(stdDeviation, MC);
		}else {
			result = dim.bigDecimalValue();
		}
		//used Professor's method here as Decimal values were not upto correct precision.
		result =  result.setScale(result.scale() + MathContext.DECIMAL128.getPrecision() - result.precision(), MathContext.DECIMAL128.getRoundingMode());

		
		return new Decimal128(result);
		
	}
	
	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
		final Scaling scaling = Scaling.valueOf(args[7]);
		final int k = Integer.valueOf(args[8]);
		
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> collection = db.getCollection(mongoCol);
		
		// TODO Your code here!
		System.out.println("\nsqlQuery --> "+ sqlQuery + "\nmongoCol -->" + mongoCol + " \n scaling -->" + scaling 
				+ "\nk ==" + k );
		java.sql.PreparedStatement pst = con.prepareStatement(sqlQuery);
		pst.setFetchSize(/* Batch size */ 20000);
		ResultSet rs = pst.executeQuery();
		System.out.print("SQL started " + new Date());
		ResultSetMetaData rsmd = (ResultSetMetaData) rs.getMetaData();
		//calculate the limits
		
		int n = rsmd.getColumnCount();
		System.out.println("\n n:   "+ n);
		
		ArrayList<Decimal128>[] relation = new ArrayList[n-1];
		for(int i =0; i< n-1; i++) {
			relation[i] = new ArrayList<>();
		}
		while(rs.next()) {
			for(int i =0; i < n - 1; i++) {
				Decimal128 dim = readAttribute(rs, "dim_" + i);
				relation[i].add(dim);
			}
		}
		pst.close();
		rs.close();
		Document limits = new Document().append("_id", "limits");
		for(int i =0; i < n-1; i++) {
			Document dim  = new Document();
			dim = calculateLimit(relation[i]);
			limits.append("dim_"+i, dim);
		}
		collection.insertOne(limits);
	
		//Scaling
		java.sql.PreparedStatement ps = con.prepareStatement(sqlQuery);
		ps.setFetchSize(/* Batch size */ 20000);
		ResultSet rst = ps.executeQuery();
		System.out.print("SQL started " + new Date());
		System.out.print("SQL ended " + new Date());
		ArrayList<Document> list = new ArrayList<>();
		int count = 0;
		int batch_size = 10000;
		while(rst.next()) {
			Long _id = rst.getLong("id");
			Document d = new Document().append("_id", "p_" +_id);
			Document point = new Document();
			Bson filters = Filters.eq("_id", "limits");
			FindIterable<Document> doc =  collection.find(filters);
			Document limits_doc = doc.first();
			System.out.println("\n doc====" + limits_doc.toString());
			for(int i =0; i < n - 1; i++) {
				Document dim_i = (Document) limits_doc.get("dim_"+i);
				System.out.println("\n sub ====" + dim_i.toString());
				Decimal128 min = (Decimal128) dim_i.get("min");
				Decimal128 max = (Decimal128) dim_i.get("max");
				Decimal128 mean = (Decimal128) dim_i.get("mean");
				Decimal128 std = (Decimal128) dim_i.get("std");
				Decimal128 dim = readAttribute(rst, "dim_" + i);
				Decimal128 scaledVal = getScaledValue(scaling, dim, min, max, mean , std);
				point.append("dim_"+i, scaledVal);
			}
			d.append("point", point);
			list.add(d);
			count++;
			if(count % batch_size == 0) {
				collection.insertMany(list);
				list = new ArrayList<>();
			}
			
		}
		if(!list.isEmpty()) {
			collection.insertMany(list);
			
		}
		list = new ArrayList<>();
		ps.close();
		rst.close();
		//adding centroids
		int size = 0;
		int batch = 500;
		for(int i =0; i < k; i++) {
			Document d = new Document().append("_id", "c_"+i);
			Document centroid = new Document();
			d.append("centroid", centroid);
			list.add(d);
			if(size % batch == 0) {
				collection.insertMany(list);
				list = new ArrayList<>();
			}
		}
		if(!list.isEmpty()) {
			collection.insertMany(list);
			
		}
		/*
		 * 
		 * The SQL query has a column named id with the id of each point (always long), and a number of columns dim_i that form the point 
		 *  with n dimensions. 
		 * 	You should store the value of each dimension as a Decimal128. In order to do so, use the readAttribute method provided.
		 * 
		 * All your computations must use BigDecimal/Decimal128. Note that x.add(y), where both x and y are BigDecimal, will not
		 *  update x, so you need to assign it to a BigDecimal, i.e., z = x.add(y).
		 *  When dividing, use MathContext.DECIMAL128 to keep the desired precision. If you implement your 
		 * 	calculations using MongoDB, do not use {$divide : [x, y]}; instead, you must do: {$multiply : [x, {$pow: [y, -1]}]}.
		 * 
		 * Each point must be of the form: {_id: p_123, point: {dim_0:_, dim_1:_, ...}}; each centroid: {_id: c_7, centroid: {}}.
		 * 
		 * Compute stat values per dimension and store them in a document whose id is 'limits'. For each dimension i, dim_i:{min:_, max:_, mean:_, std:_}.
		 * 	Note that you can use Java or MongoDB to compute these. There is a stdDevPop in MongoDB to compute standard deviation; unfortunately, it does
		 * 	not return Decimal128, so you need to find an alternate way.
		 * 
		 * Using the limits, you must scale the value using MinMax, Mean or ZScore according to the input. This only applies to the points.
		 * 
		 */
		
			
		
		// TODO End of your code!
		
		client.close();
		con.close();
	}
	
	private static Decimal128 readAttribute(ResultSet rs, String label) throws SQLException {
		// From: https://stackoverflow.com/questions/9482889/set-specific-precision-of-a-bigdecimal
		BigDecimal x = rs.getBigDecimal(label);
		x = x.setScale(x.scale() + MathContext.DECIMAL128.getPrecision() - x.precision(), MathContext.DECIMAL128.getRoundingMode());
		return new Decimal128(x);
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
