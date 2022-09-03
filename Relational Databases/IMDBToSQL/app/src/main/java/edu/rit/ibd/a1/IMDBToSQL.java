package edu.rit.ibd.a1;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class IMDBToSQL {
	private static final String NULL_STR = "\\N";
	private static int cnt =0;

	private static String SQL_CREATE_MOVIE = "CREATE TABLE Movie" 
			+ "("
			+ " id INTEGER,"
			+ " ptitle varchar(?) ,"
			+ " otitle varchar(?) ,"
			+ " adult BOOLEAN ," 
			+ "year INTEGER ," 
			+ "runtime INTEGER ," 
			+ "rating FLOAT ,"
			+ "totalvotes INTEGER ,"
			+ " PRIMARY KEY (id)"
			+ ");";

			private static String SQL_INSERT_MOVIE = "INSERT INTO  Movie(id, ptitle, otitle, adult, year, runtime) VALUES(?, ?, ?, ?, ?, ?)";
			private static String SQL_CREATE_GENRE = "CREATE TABLE Genre" + "(" + " id INTEGER AUTO_INCREMENT,"
					+ "name varchar(200)," + "UNIQUE(name)," + "PRIMARY KEY (id)" + ");";
			private static String SQL_CREATE_MOVIE_GENRE = "CREATE TABLE MovieGenre ("
					+"mid INTEGER, "
					+ "gid INTEGER,"
					+ "PRIMARY KEY(mid, gid)"
					+ ");";
			private static String SQL_INSERT_MOVIE_GENRE = "INSERT IGNORE INTO MovieGenre (mid, gid) VALUES(?, (select id FROM Genre where name = ?))";
			private static String SQL_INSERT_GENRE = "INSERT INTO Genre(name) SELECT * from (select ? as g) as tmp where not exists (select g from Genre where name = g)";
			private static String SQL_CREATE_PERSON = "CREATE TABLE Person" + "(" + "id INTEGER," + "name varchar(?),"
					+ "byear INTEGER," + "dyear INTEGER," + "PRIMARY KEY (id)" + ");";

			private static String SQL_INSERT_PERSON = "INSERT INTO Person(id, name, byear, dyear) VALUES(?,?,?,?)";
			
			private static String SQL_CREATE_KNOWNFOR = "CREATE TABLE KNOWNFOR (" + "pid INTEGER," + "mid INTEGER, "
					+ "PRIMARY KEY (pid, mid));";
			private static String SQL_INSERT_KNOWNFOR = "INSERT INTO KNOWNFOR(pid, mid) VALUES(?, ?)";
			private static String SQL_CREATE_ACTOR = "CREATE TABLE Actor (" + "pid INTEGER," + "mid INTEGER, "
					+ "PRIMARY KEY (pid, mid));";
			private static String SQL_CREATE_DIRECTOR = "CREATE TABLE Director (" + "pid INTEGER," + "mid INTEGER, "
					+ "PRIMARY KEY (pid, mid));";
			private static String SQL_CREATE_PRODUCER = "CREATE TABLE Producer (" + "pid INTEGER," + "mid INTEGER, "
					+ "PRIMARY KEY (pid, mid));";
			private static String SQL_CREATE_WRITER = "CREATE TABLE Writer (" + "pid INTEGER," + "mid INTEGER, "
					+ "PRIMARY KEY (pid, mid));";
			
			private static String SQL_INSERT_ACTOR = "INSERT IGNORE INTO Actor(pid, mid) VALUES(?, ?)";
			private static String SQL_INSERT_DIRECTOR = "INSERT IGNORE INTO Director(pid, mid) VALUES(?, ?)";
			private static String SQL_INSERT_PRODUCER = "INSERT IGNORE INTO Producer(pid, mid) VALUES(?, ?)";
			private static String SQL_INSERT_WRITER = "INSERT IGNORE INTO Writer(pid, mid) VALUES(?, ?)";
			
			private static String SQL_DELETE_ACTOR_MOVIE = "DELETE FROM Actor as A WHERE NOT EXISTS (SELECT id FROM Movie as M WHERE A.mid = M.id)";
			private static String SQL_DELETE_DIRECTOR_MOVIE =  "DELETE FROM Director as D WHERE NOT EXISTS (SELECT id FROM Movie as M WHERE D.mid = M.id)";
			private static String SQL_DELETE_PRODUCER_MOVIE = "DELETE FROM Producer as P WHERE NOT EXISTS (SELECT id FROM Movie as M WHERE P.mid = M.id)";
			private static String SQL_DELETE_WRITER_MOVIE = "DELETE FROM Writer as W WHERE NOT EXISTS (SELECT id FROM Movie as M WHERE W.mid = M.id)";
			
			private static String SQL_DELETE_ACTOR_PERSON = "DELETE FROM Actor as A WHERE NOT EXISTS (SELECT id FROM Person as P WHERE A.pid = P.id)";
			private static String SQL_DELETE_DIRECTOR_PERSON =  "DELETE FROM Director as D WHERE NOT EXISTS (SELECT id FROM Person as P WHERE D.pid = P.id)";
			private static String SQL_DELETE_PRODUCER_PERSON = "DELETE FROM Producer as PR WHERE NOT EXISTS (SELECT id FROM Person as P WHERE PR.pid = P.id)";
			private static String SQL_DELETE_WRITER_PERSON = "DELETE FROM Writer as W WHERE NOT EXISTS (SELECT id FROM Person as P WHERE W.pid = P.id)";
			private static String SQL_UPDATE_MOVIE = "UPDATE Movie SET rating = ?, totalVotes = ? WHERE id = ?";

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String folderToIMDBGZipFiles = args[3];
		final int BATCH_SIZE = 3000;
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);

		con.setAutoCommit(false);
		// ------------------------------MOVIE AND GENRE--------------
		// TABLE---------------------------------------
		// TITLE.BASICS.TSV
//
		PreparedStatement stM = con.prepareStatement("DROP TABLE IF EXISTS MOVIE, GENRE;");
		stM.execute();
		con.commit();
		stM.close();
		
		PreparedStatement st = con.prepareStatement(SQL_CREATE_MOVIE);
		PreparedStatement createGenre = con.prepareStatement(SQL_CREATE_GENRE);
//		
//		// Load movies.
		InputStream gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));
		Scanner readMovie = new Scanner(gzipStream, "UTF-8");
		
		if(readMovie.hasNextLine()) {
			readMovie.nextLine();
	}
		int maxLengthMovieP = 0, maxOriginalTitle =0;
		while (readMovie.hasNextLine()) {
			String line = readMovie.nextLine();

			// Split the line.
			String[] splitLine =line.split("\t");
			
			if(splitLine[2].length() > maxLengthMovieP) {
				maxLengthMovieP = splitLine[2].length();
			}
			if(splitLine[3].length() > maxOriginalTitle) {
				maxOriginalTitle = splitLine[3].length();
			}

		}
		st.setInt(1, maxLengthMovieP + 100);
		st.setInt(2, maxOriginalTitle + 100);
		st.execute();
		createGenre.execute();
		readMovie.close();

		// Leftovers.

		con.commit();
		st.close();
		createGenre.close();
		gzipStream.close();
		//Inserting elements 
		PreparedStatement insertMovie = con.prepareStatement(SQL_INSERT_MOVIE);
		PreparedStatement insertGenre = con.prepareStatement(SQL_INSERT_GENRE);
		InputStream gzipStream2 = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));
		Scanner insert = new Scanner(gzipStream2, "UTF-8");
		int idGenre = 0;
		if(insert.hasNextLine()) {
			insert.nextLine();
;		}
		
		while (insert.hasNextLine()) {
			String line = insert.nextLine();
			// Split the line.
			//id
			String[] splitLine =line.split("\t");
			if(!splitLine[1].equals("movie")) {
				continue;
			}
			insertMovie.setInt(1,Integer.parseInt(splitLine[0].substring(2)));
			
			//ptitle
			if(splitLine[2].equals(NULL_STR)) {
				insertMovie.setNull(2, Types.VARCHAR);
			}else {
				insertMovie.setString(2, splitLine[2]);
			}
			//otitle
			if(splitLine[3].equals(NULL_STR)) {
				insertMovie.setNull(3, Types.VARCHAR);
			}else {
				insertMovie.setString(3, splitLine[3]);
			}
			//isAdult
			if(splitLine[4].equals(NULL_STR)) {
				insertMovie.setNull(4, Types.BOOLEAN);
			}else {
				if(Integer.parseInt(splitLine[4]) == 0) {
					insertMovie.setBoolean(4, false);
				}else {
					insertMovie.setBoolean(4, true);
				}
				
			}
			//year == start year
			if(splitLine[5].equals(NULL_STR)) {
				insertMovie.setNull(5, Types.INTEGER);
			}else {
				insertMovie.setInt(5, Integer.parseInt(splitLine[5]));
			}
			//runtime
			if(splitLine[7].equals(NULL_STR)) {
				insertMovie.setNull(6, Types.INTEGER);
			}else {
				insertMovie.setInt(6, Integer.parseInt(splitLine[7]));
			}
			// GENRE POPULATION
			if(!splitLine[8].equals(NULL_STR)) {
				String[] genre = splitLine[8].split(",");
				for(String gen: genre) {
					
					insertGenre.setString(1, gen);
					insertGenre.addBatch();
				}
			}
			cnt++;
			insertMovie.addBatch();
			
			if(cnt % BATCH_SIZE == 0) {
				insertMovie.executeBatch();
				insertGenre.executeBatch();
				con.commit();
			}
		
}

		// TODO 0: End of your code.
		 cnt = 0;
		insertMovie.executeBatch();
		insertGenre.executeBatch();
		con.commit();

		insertMovie.close();
		insertGenre.close();
		// ---------------UPDATE RATES AND TOTAL VOTES---------------
//		int cnt = 0;
		PreparedStatement updateMovie = con.prepareStatement(SQL_UPDATE_MOVIE);
		InputStream gzipStreamRating = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.ratings.tsv.gz"));
		Scanner update = new Scanner(gzipStreamRating, "UTF-8");
		if(update.hasNextLine()) {
			update.nextLine();
	 }
		while(update.hasNextLine()) {
			String line = update.nextLine();
			String[] splitLine = line.split("\t");
			updateMovie.setFloat(1, Float.parseFloat(splitLine[1]));
			updateMovie.setInt(2, Integer.parseInt(splitLine[2]));
			updateMovie.setInt(3, Integer.parseInt(splitLine[0].substring(2)) );
			cnt++;
			updateMovie.addBatch();
			if(cnt % BATCH_SIZE == 0) {
				updateMovie.executeBatch();
				con.commit();
			}
			
			
		}
		cnt = 0;
		updateMovie.executeBatch();
		con.commit();
		updateMovie.close();
		gzipStreamRating.close();
		update.close();
		//-----------------------MOVIEGENRE TABLE ENTRYYYYYYYYY FROM FILE----------------------
		PreparedStatement dropMovieGenre = con.prepareStatement("DROP TABLE IF EXISTS MovieGenre ;");
		dropMovieGenre.execute();
		con.commit();
		dropMovieGenre.close();
		//create table
		PreparedStatement createMovieGenre = con.prepareStatement(SQL_CREATE_MOVIE_GENRE);
		createMovieGenre.execute();
		con.commit();
		createMovieGenre.close();
		//insert movie genre query
		PreparedStatement insertMovieGenre = con.prepareStatement(SQL_INSERT_MOVIE_GENRE);
		//read file
		InputStream gzipStreamMovieGenre = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));
		Scanner readMovieGenre = new Scanner(gzipStreamMovieGenre, "UTF-8");
//		int cnt =0;
		if(readMovieGenre.hasNextLine()) {
			readMovieGenre.nextLine();
	}
		while(readMovieGenre.hasNextLine() ) {
			String line = readMovieGenre.nextLine();
			String[] splitLine = line.split("\t");
			if(!splitLine[1].equals("movie")) { // if not movie, skip
				continue;
			}
			if(!splitLine[8].equals(NULL_STR)) {
				String[] genres = splitLine[8].split(",");
				for(String genre : genres) {
					insertMovieGenre.setInt(1,Integer.parseInt(splitLine[0].substring(2))); //mid
					insertMovieGenre.setString(2, genre);
					cnt++;
					insertMovieGenre.addBatch();
				}
			}
			if(cnt % BATCH_SIZE == 0) {
				insertMovieGenre.executeBatch();
				con.commit();
			}
			
		}
		cnt=0;
		insertMovieGenre.executeBatch();
		con.commit();
		insertMovieGenre.close();
		gzipStreamMovieGenre.close();
		readMovieGenre.close();
		////		 -------------------------PERSON TABLE------------------------------
		PreparedStatement stP = con.prepareStatement("DROP TABLE IF EXISTS PERSON, KNOWNFOR;");
		stP.execute();
		con.commit();
		stP.close();
		PreparedStatement createKnownFor = con.prepareStatement(SQL_CREATE_KNOWNFOR);
		PreparedStatement createPerson = con.prepareStatement(SQL_CREATE_PERSON);
		InputStream gzipStreamPerson = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"name.basics.tsv.gz"));
		Scanner readName = new Scanner(gzipStreamPerson, "UTF-8");
		if(readName.hasNextLine()) {
			readName.nextLine();
		}
		int maxLengthName= 0;
		while(readName.hasNextLine()) {
			String line = readName.nextLine();
			String[] splitName = line.split("\t");
			if(splitName[1].length() > maxLengthName) {
				maxLengthName = splitName[1].length();
			}
		}
		createPerson.setInt(1, maxLengthName + 100);
		createPerson.execute();
		createKnownFor.execute();
		con.commit();
		createPerson.close();
		createKnownFor.close();
		readName.close();
		gzipStreamPerson.close();
		//POPULATE FILE
//		int cnt = 0;
		PreparedStatement insertKnownFor = con.prepareStatement(SQL_INSERT_KNOWNFOR);
		PreparedStatement insertPerson = con.prepareStatement(SQL_INSERT_PERSON);
		InputStream gzipStreamPersonInsert = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"name.basics.tsv.gz"));
		Scanner readPerson = new Scanner(gzipStreamPersonInsert, "UTF-8");
		if(readPerson.hasNextLine()) {
			readPerson.nextLine();
		}
		while(readPerson.hasNextLine()) {
			String line = readPerson.nextLine();
			String[] splitPerson = line.split("\t");
			//id IN PERSON TABLE AND KNOWN FOR TABLE
			insertPerson.setInt(1, Integer.parseInt(splitPerson[0].substring(2)));
			
			if(splitPerson[1].equals(NULL_STR )) {
				insertPerson.setNull(2, Types.VARCHAR);
			}else {
				insertPerson.setString(2, splitPerson[1]);
			}
			if(splitPerson[2].equals(NULL_STR )) {
				insertPerson.setNull(3, Types.INTEGER);
			}else {
				insertPerson.setInt(3, Integer.parseInt(splitPerson[2]));
			}
			if(splitPerson[3].equals(NULL_STR )) {
				insertPerson.setNull(4, Types.INTEGER);
			}else {
				insertPerson.setInt(4, Integer.parseInt(splitPerson[3]));
			}
			// INSERTION OF MID INTO KNOWNFOR
			if(!splitPerson[5].equals(NULL_STR )) {
				String[] knownForTitles = splitPerson[5].split(",");
				insertKnownFor.setInt(1, Integer.parseInt(splitPerson[0].substring(2)));
				for(String str: knownForTitles) {
					insertKnownFor.setInt(2, Integer.parseInt(str.substring(2)));
					insertKnownFor.addBatch();
				}
			}
			cnt++;
			insertPerson.addBatch();
			
			if(cnt % BATCH_SIZE == 0) {
				insertPerson.executeBatch();
				insertKnownFor.executeBatch();
				con.commit();
			}
			
			
		}
		cnt = 0;
		insertPerson.executeBatch();
		insertKnownFor.executeBatch();
		con.commit();
		insertPerson.close();
		insertKnownFor = con.prepareStatement("DELETE FROM KNOWNFOR as K WHERE NOT EXISTS (SELECT ID FROM MOVIE as M WHERE K.mid = M.ID)");
		insertKnownFor.execute();
		con.commit();
		insertKnownFor.close();
		gzipStreamPersonInsert.close();
		readPerson.close();
//		 -------------ACTOR----DIRECTOR---------PRODUCER----------WRITER-------------------
		PreparedStatement stA = con.prepareStatement("DROP TABLE IF EXISTS ACTOR, DIRECTOR, WRITER, PRODUCER;");
		stA.execute();
		con.commit();
		stA.close();
		PreparedStatement createActor = con.prepareStatement(SQL_CREATE_ACTOR);
		createActor.execute();
		con.commit();
		PreparedStatement createDirector = con.prepareStatement(SQL_CREATE_DIRECTOR);
		createDirector.execute();
		con.commit();
		PreparedStatement createProducer = con.prepareStatement(SQL_CREATE_PRODUCER);
		createProducer.execute();
		con.commit();
		PreparedStatement createWriter = con.prepareStatement(SQL_CREATE_WRITER);
		createWriter.execute();
		con.commit();
		//close all statements
		createActor.close();
		createDirector.close();
		createProducer.close();
		createWriter.close();
		PreparedStatement insertActor = con.prepareStatement(SQL_INSERT_ACTOR);
		PreparedStatement insertDirector = con.prepareStatement(SQL_INSERT_DIRECTOR);
		PreparedStatement insertProducer = con.prepareStatement(SQL_INSERT_PRODUCER);
		PreparedStatement insertWriter = con.prepareStatement(SQL_INSERT_WRITER);
		InputStream gzipStreamProfession = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
		Scanner readProfession = new Scanner(gzipStreamProfession, "UTF-8");
		if (readProfession.hasNextLine()) {
			readProfession.nextLine();
		}
//		int cnt = 0;
		while (readProfession.hasNextLine()) {
			String line = readProfession.nextLine();
			String[] splitLine = line.split("\t");
			String category = splitLine[3].toLowerCase();
			switch (category) {
			case "self":
				insertActor.setInt(1, Integer.parseInt(splitLine[2].substring(2)));
				insertActor.setInt(2, Integer.parseInt(splitLine[0].substring(2)));
				insertActor.addBatch();
				break;
			case "actor":
				insertActor.setInt(1, Integer.parseInt(splitLine[2].substring(2)));
				insertActor.setInt(2, Integer.parseInt(splitLine[0].substring(2)));
				insertActor.addBatch();
				break;
			case "actress":
				insertActor.setInt(1, Integer.parseInt(splitLine[2].substring(2)));
				insertActor.setInt(2, Integer.parseInt(splitLine[0].substring(2)));
				insertActor.addBatch();
				break;

			case "director":
				insertDirector.setInt(1, Integer.parseInt(splitLine[2].substring(2)));
				insertDirector.setInt(2, Integer.parseInt(splitLine[0].substring(2)));
				insertDirector.addBatch();
				break;

			case "producer":
				insertProducer.setInt(1, Integer.parseInt(splitLine[2].substring(2)));
				insertProducer.setInt(2, Integer.parseInt(splitLine[0].substring(2)));
				insertProducer.addBatch();
				break;

			case "writer":
				insertWriter.setInt(1, Integer.parseInt(splitLine[2].substring(2)));
				insertWriter.setInt(2, Integer.parseInt(splitLine[0].substring(2)));
				insertWriter.addBatch();
				break;

			default:
				break;
			}
			cnt++;
			if (cnt % BATCH_SIZE == 0) {
				insertActor.executeBatch();
				insertDirector.executeBatch();
				insertProducer.executeBatch();
				insertWriter.executeBatch();
				con.commit();
			}

		}
		cnt = 0;
		insertActor.executeBatch();
		insertDirector.executeBatch();
		insertProducer.executeBatch();
		insertWriter.executeBatch();
		con.commit();
		
		
		//READING OF CREW FILE FOR LEFTOVER DIRECTORA NAD WRITERS
		InputStream gzipStreamCrew = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.crew.tsv.gz"));
		Scanner readCrew = new Scanner(gzipStreamCrew, "UTF-8");
		if (readCrew.hasNextLine()) {
			readCrew.nextLine();
		}
		while(readCrew.hasNextLine()) {
			String line = readCrew.nextLine();
			String[] splitLine = line.split("\t");
			if(!splitLine[1].equals(NULL_STR)) {
				insertDirector.setInt(2, Integer.parseInt(splitLine[0].substring(2)));  //mid
				String[] directors = splitLine[1].split(",");
				for(String director: directors) {
					insertDirector.setInt(1, Integer.parseInt(director.substring(2))); //pid
					insertDirector.addBatch();
					cnt++;
				}
				
			}
			if(!splitLine[2].equals(NULL_STR)) {
				insertWriter.setInt(2, Integer.parseInt(splitLine[0].substring(2)));  //mid
				String[] writers = splitLine[2].split(",");
				for(String writer: writers) {
					insertWriter.setInt(1, Integer.parseInt(writer.substring(2)));  //pid
					insertWriter.addBatch();
					cnt++;
				}
				
			}
			
			if(cnt % BATCH_SIZE == 0) {
				insertDirector.executeBatch();
				insertWriter.executeBatch();
				con.commit();
			}
			
		}
		cnt =0;
		insertDirector.executeBatch();
		insertWriter.executeBatch();
		con.commit();
		//DELETION OF ENTRIES WHICH ARE NOT MOVIES
		
		//ACTOR
		insertActor = con.prepareStatement(SQL_DELETE_ACTOR_MOVIE);
		insertActor.execute();
		con.commit();
		
		insertActor = con.prepareStatement(SQL_DELETE_ACTOR_PERSON);
		insertActor.execute();
		con.commit();
		
		//PRODUCER
		insertProducer = con.prepareStatement(SQL_DELETE_PRODUCER_MOVIE);
		insertProducer.execute();
		con.commit();
		
		insertProducer = con.prepareStatement(SQL_DELETE_PRODUCER_PERSON);
		insertProducer.execute();
		con.commit();
		
		//DIRECTOR
		insertDirector = con.prepareStatement(SQL_DELETE_DIRECTOR_MOVIE);
		insertDirector.execute();
		con.commit();
		
		insertDirector = con.prepareStatement(SQL_DELETE_DIRECTOR_PERSON);
		insertDirector.execute();
		con.commit();
		
		//WRITER
		insertWriter = con.prepareStatement(SQL_DELETE_WRITER_MOVIE);
		insertWriter.execute();
		con.commit();
		
		insertWriter = con.prepareStatement(SQL_DELETE_WRITER_PERSON);
		insertWriter.execute();
		con.commit();
				// leftovers
		insertActor.close();
		insertDirector.close();
		insertProducer.close();
		insertWriter.close();
		gzipStreamProfession.close();
		readProfession.close();
		
		con.close();
	}
}
