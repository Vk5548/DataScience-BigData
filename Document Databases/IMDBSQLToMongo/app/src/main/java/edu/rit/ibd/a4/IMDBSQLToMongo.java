package edu.rit.ibd.a4;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IMDBSQLToMongo {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String mongoDBURL = args[3];
		final String mongoDBName = args[4];
//		String dbURL = "jdbc:mysql://localhost:3306/Movies?useCursorFetch=true";
//		final String user = "vaidehi";
//		final String pwd = "vk123456";
//		final String mongoDBURL = "mongodb://localhost:27017/";
//		final String mongoDBName = "IMDB";
		System.out.println(new Date() + " -- Started");

		Connection con = DriverManager.getConnection(dbURL, user, pwd);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		// TODO 0: Your code here!

		/*
		 * 
		 * Everything in MongoDB is a document (both data and queries). To create a
		 * document, I use primarily two options but there are others if you ask the
		 * Internet. You can use org.bson.Document as follows:
		 * 
		 * Document d = new Document(); d.append("name_of_the_field", value);
		 * 
		 * The type of the field will be the conversion of the Java type of the value.
		 * 
		 * Another option is to parse a string representing the document:
		 * 
		 * Document d = Document.parse("{ _id:1, name:\"Name\" }");
		 * 
		 * It will parse only well-formed documents. Note that the previous approach
		 * will use the Java data types as the types of the pieces of data to insert in
		 * MongoDB. However, the latter approach will not have that info as everything
		 * is a string; therefore, be mindful of these differences and use the approach
		 * it will fit better for you.
		 * 
		 * If you wish to create an embedded document, you can use the following:
		 * 
		 * Document outer = new Document(); Document inner = new Document();
		 * outer.append("doc", inner);
		 * 
		 * To connect to a MongoDB database server, use the getClient method above. If
		 * your server is local, just provide "None" as input.
		 * 
		 * You must extract data from MySQL and load it into MongoDB. Note that, in
		 * general, the data in MongoDB is denormalized, which means that it includes
		 * redundancy. You must think of ways of extracting such redundant data in
		 * batches, that is, you should think of a bunch of queries that will retrieve
		 * the whole database in a format it will be convenient for you to load in
		 * MongoDB. Performing many small SQL queries will not work.
		 * 
		 * If you execute a SQL query that retrieves large amounts of data, all data
		 * will be retrieved at once and stored in main memory. To avoid such behavior,
		 * the JDBC URL will have the following parameter: 'useCursorFetch=true'
		 * (already added by the grading software). Then, you can control the number of
		 * tuples that will be retrieved and stored in memory as follows:
		 * 
		 * PreparedStatement st = con.prepareStatement("SELECT ...");
		 * st.setFetchSize(batchSize);
		 * 
		 * where batchSize is the number of rows.
		 * 
		 * Null values in MySQL must be translated as documents without such fields.
		 * 
		 * Once you have composed a specific document with the data retrieved from
		 * MySQL, insert the document into the appropriate collection as follows:
		 * 
		 * MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		 * 
		 * ...
		 * 
		 * Document d = ...
		 * 
		 * ...
		 * 
		 * col.insertOne(d);
		 * 
		 * You should focus first on inserting all the documents you need (movies and
		 * people). Once those documents are already present, you should deal with the
		 * mapping relations. To do so, MongoDB is optimized to make small updates of
		 * documents referenced by their keys (different than MySQL). As a result, it is
		 * a good idea to update one document at a time as follows:
		 * 
		 * PreparedStatement st = con.prepareStatement("SELECT ..."); // Select from
		 * mapping table. st.setFetchSize(batchSize); ResultSet rs = st.executeQuery();
		 * while (rs.next()) {
		 * col.updateOne(Document.parse("{ _id : "+rs.get(...)+" }"),
		 * Document.parse(...)); ...
		 * 
		 * The updateOne method updates one single document based on the filter
		 * criterion established in the first document (the _id of the document to fetch
		 * in this case). The second document provided as input is the update operation
		 * to perform. There are several updates operations you can perform (see
		 * https://docs.mongodb.com/v3.6/reference/operator/update/). If you wish to
		 * update arrays, $push and $addToSet are the best options but have slightly
		 * different semantics. Make sure you read and understand the differences
		 * between them.
		 * 
		 * When dealing with arrays, another option instead of updating one by one is
		 * gathering all values for a specific document and perform a single update.
		 * 
		 * Note that array fields that are empty are not allowed, so you should not
		 * generate them.
		 * 
		 */

		// MOVIE TABLE FROM SQL

		db.getCollection("Movies").drop();
		db.createCollection("Movies");
		MongoCollection<Document> colM = db.getCollection("Movies");
		;
		// MoviesDenorm
		db.getCollection("MoviesDenorm").drop();
		db.createCollection("MoviesDenorm");
		MongoCollection<Document> colMoviesD = db.getCollection("MoviesDenorm");
		// if it doesn't exist. It will be created

		// Try to use few queries that retrieve big chunks of
//		data rather than many queries that retrieve small pieces of data.

		PreparedStatement movieSt = con.prepareStatement("SELECT * from movie");
		movieSt.setFetchSize(/* Batch size */ 50000);
		ResultSet rsMovie = movieSt.executeQuery();
		System.out.print("Movie started " + new Date());
		List<Document> listM = new ArrayList<Document>();
		List<Document> listMD = new ArrayList<Document>();
		int count = 0;
		while (rsMovie.next()) {
			Document d = new Document();
			Document md = new Document();
			// id
			d.append("_id", rsMovie.getInt("id"));
			md.append("_id", rsMovie.getInt("id"));
			listMD.add(md);
			// ptilte
			String ptitle = rsMovie.getString("ptitle");
			if (ptitle != null) {
				d.append("ptitle", ptitle);
			}
			// otitle
			String otitle = rsMovie.getString("otitle");
			if (otitle != null) {
				d.append("otitle", otitle);
			}
			// adult
			boolean adult = rsMovie.getBoolean("adult");
			d.append("adult", adult);
			// year
			Integer year = rsMovie.getInt("year");
			if (year != null && year != 0) {
				d.append("year", year);
			}
			// runtime
			Integer runtime = rsMovie.getInt("runtime");
			if (runtime != null && runtime != 0) {
				d.append("runtime", runtime);
			}
			// get rating
			BigDecimal rating = rsMovie.getBigDecimal("rating");
			if (rating != null) {
				d.append("rating", new Decimal128(rating));
			}
//			Decimal128 rating = new Decimal128(rs.getBigDecimal("rating"));

			// get totalVotes
			Integer totalVotes = rsMovie.getInt("totalvotes");
			if (totalVotes != null && totalVotes != 0) {
				d.append("totalvotes", totalVotes);
			}
			listM.add(d);
			count++;
			if (count % 20000 == 0) {
				colM.insertMany(listM);
				colMoviesD.insertMany(listMD);
				listM = new ArrayList<>();
				listMD = new ArrayList<>();
			}
			// d.append("_id", rs.getInt("id))
//			col.updateOne(d, d); //(filter, update)
//			col.updateMany(d, d);
			// If something is NULL, then, do not include the field!

			// To deal with float attributes, use the code below to retrieve big decimals
			// for attribute x in MySQL and create Decimal128 in MongoDB.
			// Decimal128 so that we have compatibility
//			x.toString();
//			col.insertOne(d);
//			col.insertMany(null);  // much faster
		}
		colM.insertMany(listM);
		colMoviesD.insertMany(listMD);
		listM = new ArrayList<>();
		listMD = new ArrayList<>();
		System.out.println("Movie Done " + new Date());
		rsMovie.close();
		movieSt.close();
//		
//		// GENRE TABLE AND MOVIE GENRE
		PreparedStatement movieGenreSt = con
				.prepareStatement("SELECT * from moviegenre as mg join genre as g" + " on mg.gid = g.id");
		movieGenreSt.setFetchSize(/* Batch size */ 50000);
		ResultSet rsMovieGenre = movieGenreSt.executeQuery();
		while (rsMovieGenre.next()) {
			Document documentToBeUpdated = new Document();
			Integer movieId = rsMovieGenre.getInt("mid");
			documentToBeUpdated.append("_id", movieId);
			String genre = rsMovieGenre.getString("name");
			Document newUpdate = new Document().append("genres", genre);
			Document add = new Document();

			add.append("$push", newUpdate);
			colM.updateOne(documentToBeUpdated, add);
		}
		movieGenreSt.close();
		rsMovieGenre.close();

		// create people collection
		db.getCollection("People").drop();
		db.createCollection("People");
		MongoCollection<Document> colPeople = db.getCollection("People");
		;
		// PeopleDenorm
		db.getCollection("PeopleDenorm").drop();
		db.createCollection("PeopleDenorm");
		MongoCollection<Document> colPeopleD = db.getCollection("PeopleDenorm");
		PreparedStatement people = con.prepareStatement("SELECT * from person");
		people.setFetchSize(/* Batch size */ 50000);
		ResultSet rsPeople = people.executeQuery();
		List<Document> list = new ArrayList<Document>();
		List<Document> listPD = new ArrayList<Document>();
		count = 0;
		while (rsPeople.next()) {
			Document d = new Document();
			// id
			Integer id = rsPeople.getInt("id");
			d.append("_id", id);
			listPD.add(d);
			// name
			String name = rsPeople.getString("name");
			d.append("name", name);
			Integer byear = rsPeople.getInt("byear");
			d.append("byear", byear);
			Integer dyear = rsPeople.getInt("dyear");
			if (dyear != 0) {
				d.append("dyear", dyear);
			}
			list.add(d);
//			colPeople.insertOne(d);
			count++;
			if (count % 20000 == 0) {
				colPeople.insertMany(list);
				colPeopleD.insertMany(listPD);
				list = new ArrayList<Document>();
				listPD = new ArrayList<Document>();
			}

		}
		colPeople.insertMany(list);
		colPeopleD.insertMany(listPD);
		list = new ArrayList<Document>();
		listPD = new ArrayList<Document>();
		rsPeople.close();
		people.close();
		// create PeopleDenormCollection
		// ACTOR TABLE
		PreparedStatement actor = con.prepareStatement("SELECT * from actor");
		actor.setFetchSize(/* Batch size */ 50000);
		ResultSet rsActor = actor.executeQuery();
		while (rsActor.next()) {
			Integer mid = rsActor.getInt("mid");
			Integer pid = rsActor.getInt("pid");
			Document movieDenorm = new Document();
			Document peopleDenorm = new Document();
			movieDenorm.append("_id", mid);
			peopleDenorm.append("_id", pid);
			Document movieUpdate = new Document().append("actors", pid);
			Document peopleUpdate = new Document().append("acted", mid);
			Document addMovie = new Document();
			Document addPeople = new Document();
			addMovie.append("$push", movieUpdate);
			addPeople.append("$push", peopleUpdate);
			colMoviesD.updateOne(movieDenorm, addMovie);
			colPeopleD.updateOne(peopleDenorm, addPeople);
		}
		actor.close();
		rsActor.close();

		// Director TABLE
		PreparedStatement director = con.prepareStatement("SELECT * from director");
		director.setFetchSize(/* Batch size */ 50000);
		ResultSet rsDirector = director.executeQuery();
		while (rsDirector.next()) {
			Integer mid = rsDirector.getInt("mid");
			Integer pid = rsDirector.getInt("pid");
			Document movieDenorm = new Document();
			Document peopleDenorm = new Document();
			movieDenorm.append("_id", mid);
			peopleDenorm.append("_id", pid);
			Document movieUpdate = new Document().append("directors", pid);
			Document peopleUpdate = new Document().append("directed", mid);
			Document addMovie = new Document();
			Document addPeople = new Document();
			addMovie.append("$push", movieUpdate);
			addPeople.append("$push", peopleUpdate);
			colMoviesD.updateOne(movieDenorm, addMovie);
			colPeopleD.updateOne(peopleDenorm, addPeople);
		}
		director.close();
		rsDirector.close();

		PreparedStatement producer = con.prepareStatement("SELECT * from producer");
		producer.setFetchSize(/* Batch size */ 50000);
		ResultSet rsProducer = producer.executeQuery();
		while (rsProducer.next()) {
			Integer mid = rsProducer.getInt("mid");
			Integer pid = rsProducer.getInt("pid");
			Document movieDenorm = new Document();
			Document peopleDenorm = new Document();
			movieDenorm.append("_id", mid);
			peopleDenorm.append("_id", pid);
			Document movieUpdate = new Document().append("producers", pid);
			Document peopleUpdate = new Document().append("produced", mid);
			Document addMovie = new Document();
			Document addPeople = new Document();
			addMovie.append("$push", movieUpdate);
			addPeople.append("$push", peopleUpdate);
			colMoviesD.updateOne(movieDenorm, addMovie);
			colPeopleD.updateOne(peopleDenorm, addPeople);
		}
		producer.close();
		rsProducer.close();

		// Writer TABLE
		PreparedStatement writer = con.prepareStatement("SELECT * from writer");
		writer.setFetchSize(/* Batch size */ 50000);
		ResultSet rsWriter = writer.executeQuery();
		while (rsWriter.next()) {
			Integer mid = rsWriter.getInt("mid");
			Integer pid = rsWriter.getInt("pid");
			Document movieDenorm = new Document();
			Document peopleDenorm = new Document();
			movieDenorm.append("_id", mid);
			peopleDenorm.append("_id", pid);
			Document movieUpdate = new Document().append("writers", pid);
			Document peopleUpdate = new Document().append("written", mid);
			Document addMovie = new Document();
			Document addPeople = new Document();
			addMovie.append("$push", movieUpdate);
			addPeople.append("$push", peopleUpdate);
			colMoviesD.updateOne(movieDenorm, addMovie);
			colPeopleD.updateOne(peopleDenorm, addPeople);
		}
		writer.close();
		rsWriter.close();
//
//		// KnownFor TABLE
		PreparedStatement known = con.prepareStatement("SELECT * from knownfor");
		known.setFetchSize(/* Batch size */ 50000);
		ResultSet rsknownfor = known.executeQuery();
		while (rsknownfor.next()) {
			Integer mid = rsknownfor.getInt("mid");
			Integer pid = rsknownfor.getInt("pid");

			Document peopleDenorm = new Document();

			peopleDenorm.append("_id", pid);

			Document peopleUpdate = new Document().append("knownfor", mid);

			Document addPeople = new Document();

			addPeople.append("$push", peopleUpdate);

			colPeopleD.updateOne(peopleDenorm, addPeople);
		}
		known.close();
		rsknownfor.close();
// TODO 0: End of your code.

		client.close();
		con.close();
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
