package edu.rit.ibd.a2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DUDQ{

	public static void main(String[] args) throws Exception {
		String url = args[0];
		String user = args[1];
		String pwd = args[2];
		String directorName = args[3];
		
		Connection con = null;
		con = DriverManager.getConnection(url, user, pwd);
		
		// TODO Make this code secure! You need to make sure that director names that exist are only found.
		PreparedStatement st = con.prepareStatement("SELECT DISTINCT id FROM Person JOIN Director WHERE pid=id AND name=? LIMIT 1");
		// TODO End of your code.
		st.setString(1, directorName);
		ResultSet rs = st.executeQuery();
		if (rs.next())
			System.out.println("Id: " + rs.getObject("id"));
		rs.close();
		st.close();
		con.close();
	}

}
