package edu.rit.ibd.a5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.bson.BsonArray;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

//my imports
import java.nio.file.Files;  
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.ne;
import com.mongodb.client.model.Projections;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;


import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class DeRefMongo {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String jsonFile = args[2];
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		// create collectiom NovieInfo
		db.getCollection("MovieInfo").drop();
		db.createCollection("MovieInfo");
		MongoCollection<Document> col = db.getCollection("MovieInfo");
		MongoCollection<Document> colMovie = db.getCollection("Movies");
		colMovie.createIndex(new Document().append("otitle", 1));

		// Read the file and load the docs in the list. Alternatively, you can also load them in a new collection in MongoDB.
		//This is the old way I read the file, but obviously had memory issues
//		String fileMovieInfo = new String(Files.readAllBytes(Paths.get(jsonFile)));
		System.out.println("Start");
//		
		//UPDATED WAY TO READ THE FILE : Quite Memory efficient
		FileReader reader = new FileReader(jsonFile);
		BufferedReader br = new BufferedReader(reader);
		Document allDocuments = Document.parse("{\"allDocuments\":" + br.readLine() + "}"); //as told by Professor in class
		List<Document> movieInfoDocs = (List<Document>) allDocuments.get("allDocuments");
		br.close();
		//pattern
		String patternString = "tt[0-9]+";
		Pattern pattern = Pattern.compile(patternString);
//		BsonArray docs = new BsonArray().parse(fileMovieInfo);  // old way, I did things because file reading was done differently
//		Iterator iter = docs.iterator();
		List<Document> list = new ArrayList<Document>();
		int count = 0;
		
		System.out.println("Movie Start " + new Date());
//		while(iter.hasNext()) {
			for(Document d : movieInfoDocs) {
			
//			String str = (String)iter.next().toString();
//			new Document();
//			Document d = Document.parse(str);
			String ttid = (String) d.get("id");
			Matcher matcher = pattern.matcher(ttid);
		    boolean matches = matcher.matches();
		    if(matches) {
		    	String resultLabel = (String) d.get("resultLabel");
		    	String title = (String) d.get("title");
		    	if(resultLabel != null) {
		    		int id = Integer.parseInt(ttid.trim().substring(2));
		    		d.append("id", id);
		    		d.append("resultLabel", resultLabel.trim());
		    		list.add(d);
		    		System.out.println(count);
		    		colMovie.updateOne(Filters.eq("_id", id), Updates.set("bechdel-test-id", resultLabel.trim()));
		    		colMovie.updateMany(Filters.eq("otitle", title), Updates.set("bechdel-test-title", resultLabel.trim()));
		    		count++;
		    		
		    		if(count % 10000 == 0) {
		    			col.insertMany(list);
		    			list = new ArrayList<>();
		    			System.out.println(count);
		    		}
		    	}
		    }
		    
		    
			
		}
		col.insertMany(list);
		System.out.println("Movie Done " + new Date());
		col.createIndex(new Document().append("id", 1));
		col.createIndex(new Document().append("title", 1));
		
		
 		
		System.out.println("Start fetching");
		FindIterable<Document> iterDoc = col.find();
	      Iterator<Document> it = iterDoc.iterator();
	      int size = 0;
	      while (it.hasNext()) {
//	    	  String str = .toString();
				Document dMoiveInfo = it.next();
				Integer id = dMoiveInfo.getInteger("id");
				String resultLabel = dMoiveInfo.getString("resultLabel");
				String title = dMoiveInfo.getString("title");
				Bson filters = Filters.and(Filters.eq("id", id),
						ne("resultLabel", resultLabel));
               long countMatchingDocuments = col.countDocuments(filters); 
//               System.out.println("id conf " + countMatchingDocuments);
               if(countMatchingDocuments != 0) {
            	   colMovie.updateOne(Filters.eq("_id", id), Updates.set("id-conflicts", countMatchingDocuments));
               }
            	   
               Bson filtersTitle = Filters.and(Filters.eq("title", title),
						ne("id", id));
               long countMatchingDocumentsTitle = col.countDocuments(filtersTitle); 
               FindIterable<Document> doc =  col.find(filtersTitle);
               System.out.println("title conf " + size++);
               if(countMatchingDocumentsTitle != 0) {
            	   colMovie.updateOne(Filters.eq("_id", id), Updates.set("title-conflicts", countMatchingDocumentsTitle));
            	   colMovie.updateMany(Filters.eq("otitle", title), Updates.set("title-conflicts", countMatchingDocumentsTitle));
//	            	   FindIterable<Document> iterDocTitle = (FindIterable<Document>) doc;
            	   Iterator<Document> itTitle = doc.iterator();
            	   while(itTitle.hasNext()) {
            		   Document subDoc = itTitle.next();
            		   Integer subId = subDoc.getInteger("id");
            		   colMovie.updateOne(Filters.eq("_id", subId), Updates.set("title-conflicts", countMatchingDocumentsTitle));
       				
            	   }
            	   
               }
//          System.out.println(doc);
//          System.out.println(countMatchingDocuments);
//          System.out.println(countMatchingDocumentsTitle);
//	          break;
	       }
		
	    colMovie.dropIndex(new Document().append("otitle", 1));
		col.dropIndex(new Document().append("id", 1));
		col.dropIndex(new Document().append("title", 1));
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
