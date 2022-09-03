package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import com.mysql.jdbc.ResultSetMetaData;

public class FDDiscovery {

	public static boolean sqlQuery(Set lhs, String rhs, int size, String relation, Connection con) throws SQLException {
		Object[] arr = lhs.toArray();
		String onQuery = "", whereQuery = "r1." + rhs + " <> r2." + rhs;
		for (Object obj : arr) {
			String str = (String) obj;
			onQuery += "r1." + str + " = r2." + str + " and ";
		}
		int index = onQuery.lastIndexOf("and");


		String sql = "Select * from " + relation + " as R1 join " + relation + " as R2 on "
				+ onQuery.substring(0, index) + " where " + whereQuery + " LIMIT 1";
		PreparedStatement st = con.prepareStatement(sql);
		ResultSet rs = st.executeQuery();
		if (!rs.next()) {
			rs.close();
			st.close();
			return true;
		}
		rs.close();
		st.close();
		return false;
	}

	public static boolean checkMinimality(Set<String> lhs, Set<Object> fds, String rhs) {
		for (int i = 1; i <= lhs.size(); i++) {
			for (Set<String> l : Sets.combinations(lhs, i)) {
				Object[] temp = l.toArray();
				Arrays.sort(temp);
				String minimal = "";
				for (Object str : temp) {
					minimal += str.toString() + ", ";
				}
				int index = minimal.lastIndexOf(",");

				if (fds.contains(minimal.substring(0, index).trim() + " -> " + rhs)) {
					return false;
				}
			}
		}

		return true;

	}

	public static boolean checkTriviality(Set lhs, String rhs) {
		Object[] temp = lhs.toArray();
		for (Object o : temp) {
			String trivial = o.toString();
			if (trivial.equals(rhs)) {
				return false;
			}
		}
		return true;

	}

	public static void main(String[] args) throws Exception {
		final String url = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String relationName = args[3];
		final String outputFile = args[4];

		Connection con = DriverManager.getConnection(url, user, pwd);

		// These are the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// These are the functional dependencies discovered.
		Set<Object> fds = new HashSet<>();

		PreparedStatement st = con.prepareStatement("SELECT * FROM " + relationName + " Limit 1");
		ResultSet rs = st.executeQuery();

		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
			String column = rs.getMetaData().getColumnName(i);

			attributes.add(rs.getMetaData().getColumnName(i));
		}

		rs.close();
		st.close();

		// Each FD has a left-hand side and a right-hand side. For LHS, start from size
		// one and keep increasing.
		for (int size = 1; size < attributes.size(); size++) {
			// Get each combination of attributes in the left-hand side of the appropriate
			// size.
			for (Set<String> leftHandSide : Sets.combinations(attributes, size)) {
				// Get the attributes in the right-hand side.
				for (String rhs : attributes) {

					// not trivial


					boolean isTrivial = checkTriviality(leftHandSide, rhs);
					boolean isMinimal = checkMinimality(leftHandSide, fds, rhs);
					if (isTrivial && isMinimal) {

						// triviality

						boolean isFd = sqlQuery(leftHandSide, rhs, size, relationName, con);
						if (isFd) {
							Object[] temp = leftHandSide.toArray();
							Arrays.sort(temp);

							String dependency = "";
							for (Object o : temp) {

								dependency += o.toString() + ", ";

							}
							int index = dependency.lastIndexOf(",");
							dependency = dependency.substring(0, index) + " -> " + rhs;

							fds.add(dependency);
						}
					}

				}

			}
		}

//		 Write to file!
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Object fd : fds)
			writer.println(fd);
		writer.close();

		con.close();
	}

}
