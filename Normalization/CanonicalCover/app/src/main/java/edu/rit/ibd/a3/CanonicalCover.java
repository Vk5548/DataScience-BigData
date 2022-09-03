package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Sets;

public class CanonicalCover{
	private static ConcurrentHashMap<Object, Object> mergeFds(ConcurrentHashMap<Object, Object> fds){
		boolean thereWasAUnion = false;
		while(true) {
			
			thereWasAUnion = false;
			for(Object fd1 : fds.keySet()) {
				for(Object fd2 : fds.keySet()) {
					if(!fd1.toString().equals(fd2.toString())) {

						String lhs1 = fd1.toString().split("->")[0];
						String lhs2 = fd2.toString().split("->")[0];
						if(lhs1.equals(lhs2)) {
							String fd = merge(fd1, fd2);
							fds.remove(fd1);
							fds.remove(fd2);
							fds.put(fd, fd);
							thereWasAUnion = true;
							break;
						}
					}
					if(thereWasAUnion)
						break;
				}
				if(thereWasAUnion)
					continue;
			}
			if(!thereWasAUnion) {
				
				break;
			}
				
		}
		return fds;
	}
	private static boolean computeClosure(ConcurrentHashMap<Object, Object> fds, Set<String> set, Set<String> ifExtra) {
	Set<String> result = new HashSet<>(set);

	
	int resultSizeInitial = result.size();
	int resultSizeLater = 0;

	while(resultSizeInitial != resultSizeLater) {

		resultSizeInitial = result.size();

		for(Object fd: fds.keySet()) {
			
			String[] split = fd.toString().split("->");
			Set<String> left = new HashSet<String>();
			for (String s: split[0].split(",")) {
				left.add(s.trim());
			}
			
				String[] secondSplit = split[1].split(",");
			
			if(result.containsAll(left)) {
				
				
				for(String str: secondSplit) {

					result.add(str.trim());
					if(result.containsAll(ifExtra)) {
						return true;
					}
				};

			}
				
			
		
			
		}
		resultSizeLater = result.size();
		
	}


	

	return false;
}

	
	public static String merge(Object fd1, Object fd2) {
		String lhs = fd1.toString().split("->")[0];
		String rhs1 = fd1.toString().split("->")[1];
		String rhs2 = fd2.toString().split("->")[1];
		String[] fd2RhsArr = rhs2.split(",");
		for(String rhs : fd2RhsArr) {
			if(!rhs1.contains(rhs)) {
				rhs1 += "," + rhs.trim();
			}
		}
		String fd = lhs + "->" + rhs1;

		return fd;
		
	}
	
	public static ConcurrentHashMap<Object, Object> computeCanonicalCover(ConcurrentHashMap<Object, Object> fds){
		fds = mergeFds(fds);

		boolean  thereWereExtraneous = false;
		
		while(true) {
			
			thereWereExtraneous = false;
			for(Object fd: fds.keySet()) {
				// if lhs is extra
				
				String[] lhs = fd.toString().split("->")[0].split(",");
 				String[] rhs = fd.toString().split("->")[1].split(",");
				Set<String> lhsSet = new HashSet<>();
				Set<String> lhsSubset = new HashSet<>();
				Set<String> rhsSet = new HashSet<>();
				Set<String> rhsSubset = new HashSet<>();
				for(String attribute: lhs) {
					
					//a, b, c -> // putting the values in hashMAp
					
					lhsSet.add(attribute.trim());
					
				}
				for(String attribute: rhs) {
					
					//a, b, c -> // putting the values in hashMAp
					
					rhsSet.add(attribute.trim());
					
				}
					if(lhs.length > 1) {
						
					
					
					System.out.println("lhsSet added : " + lhsSet);
					for(Object attribute: lhsSet) {
						// if str is extraneous

						Set<String> temp = new HashSet<>();
						temp.add(attribute.toString().trim());
						lhsSubset = Sets.difference(lhsSet, temp);
						
						//call the function to compute closure
						boolean ifExtra = computeClosure(fds, lhsSubset, rhsSet);
						
						if(ifExtra) {
							String lhsSubsetString = lhsSubset.toString().replace("[", "");
							lhsSubsetString = lhsSubsetString.replace("]", "");
							lhsSubsetString = lhsSubsetString.replace(", ", ",");
							String SubsetStr = lhsSubsetString +  "->" + fd.toString().split("->")[1];
							
							
							fds.remove(fd);
							fds.put(SubsetStr, SubsetStr);
							thereWereExtraneous = true;
							fds = mergeFds(fds);
							break;
						}
					}
				}
					if(thereWereExtraneous) {
						continue;
					}
					thereWereExtraneous = false;
					// If RHS is extra
					if(rhs.length > 1)
					{
					
						
						
						for(Object attribute: rhsSet) {
							Set<String> temp = new HashSet<>();
							temp.add(attribute.toString());
							rhsSubset = Sets.difference(rhsSet, temp);
							
							ConcurrentHashMap<Object, Object> fds_temp = new ConcurrentHashMap<Object, Object>(fds);
							String rhsSubsetString = rhsSubset.toString().replace("[", "");
							rhsSubsetString = rhsSubsetString.replace("]", "");
							rhsSubsetString = rhsSubsetString.replace(", ", ",");
							String tempSubsetStr = fd.toString().split("->")[0] +  "->" + rhsSubsetString;
							fds_temp.remove(fd);
							fds_temp.put(tempSubsetStr, tempSubsetStr);
							boolean ifExtra = computeClosure(fds_temp, lhsSet, temp);
							if(ifExtra) {
								fds = fds_temp;
								thereWereExtraneous = true;
								fds = mergeFds(fds);
								break;
							}
							
						}
					}
				
			}
			if(!thereWereExtraneous)
				break;
		}
		return fds;
	}
	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String outputFile = args[2];
		

		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// This stores the functional dependencies provided as input. This will be the output as well.
		Set<Object> fds = new HashSet<>();
		ConcurrentHashMap<Object, Object> canonicalCover = new ConcurrentHashMap<>();
		
		// Parse relation.
		int startIndex = relation.indexOf("(");
		int lastIndex = relation.lastIndexOf(")");
		
		String[] split = relation.substring(startIndex + 1, lastIndex).split(",");
		
		for(String str: split)
		// Parse attributes.
			attributes.add(str.trim());
		
		
		// Parse FDs.
		String[] getFds = fdsStr.split(";");
		String leftHandSide = "", rightHandSide = "";
		
		for(String str: getFds) {

			fds.add(str);
		}
		
		ConcurrentHashMap<Object, Object> mapFd= new ConcurrentHashMap<>();
		for(Object fd: fds) {
			String resL= "", resR ="";
			String[] lhs = fd.toString().split("->")[0].trim().split(",");
			String[] rhs = fd.toString().split("->")[1].trim().split(",");
			for(String l: lhs)
				resL += l.trim() + ",";
			int indexL = resL.lastIndexOf(",");
			resL = resL.substring(0, indexL);
			for(String l: rhs)
				resR += l.trim() + ",";
			int indexR = resR.lastIndexOf(",");
			resR = resR.substring(0, indexR);
			String full = resL + "->" + resR;
			mapFd.put(full, full);
		}
			
		
		
		// Remove extraneous attributes, each one either in the left-hand or right-hand sides.
		canonicalCover = computeCanonicalCover(mapFd);
		
		// TODO 0: End of your code.
		
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Object fd : canonicalCover.keySet()) {
			String newFd = fd.toString();

			String[] fdLhs = newFd.split("->")[0].split(",");
			String[] fdRhs = newFd.split("->")[1].split(",");
			
			Arrays.sort(fdLhs);
			Arrays.sort(fdRhs);
			newFd = (Arrays.toString(fdLhs).replace("[", "").replace("]", "") + " -> " + Arrays.toString(fdRhs).replace("[", "").replace("]", ""));

			
			writer.println(newFd);
		}

			
		
		writer.close();
	}

}
