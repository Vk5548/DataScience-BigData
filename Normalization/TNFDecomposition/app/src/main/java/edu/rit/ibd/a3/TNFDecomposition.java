package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Sets;

public class TNFDecomposition {
	private static ConcurrentHashMap<Set<String>, Set<String>> fdsIntoMap(Set<Object> fds){
		ConcurrentHashMap<Set<String>, Set<String>> result = new ConcurrentHashMap<>();
		for(Object fd: fds) {
			HashSet<String> lhs = new HashSet<>();
			HashSet<String> rhs = new HashSet<>();
			String[] split = fd.toString().split("->");
			for(String attr: split[0].split(",")) {
				lhs.add(attr.trim());
			}
			for(String attr: split[1].split(",")) {
				rhs.add(attr.trim());
			}
			result.put(lhs, rhs);
		}
		return result;
		
	}
	private static boolean isLhsSuperKey(Set<String> lhs, List<Set<String>> cks) {
		for(Set<String> ck : cks) {
			if(lhs.containsAll(ck)) {
				return true;
			}
		}
		return false;
	}
	private static boolean checkForThird(Set<String> lhs, Set<String> rhs, List<Set<String>> cks) {
		for(Set<String> ck: cks) {
			Set<String> difference = Sets.difference(rhs, lhs);
			if(ck.containsAll(difference)) {
				return true;
			}
		}
		return false;
		
	}
	private static boolean checkForTnf(ConcurrentHashMap<Set<String>, Set<String>> fdsMap, List<Set<String>> cks){
		
		boolean check = false;
		for(Set<String> lhs: fdsMap.keySet()) {
			Set<String> rhs = fdsMap.get(lhs);
			boolean isTrivial = false, isASuper = false, isBMinusA = false;
			if(lhs.contains(rhs)) //triviality
			{
				isTrivial = true;
			}else {
				
			}
			
			if(isLhsSuperKey(lhs, cks)) {
				isASuper = true;
				// B - A in some candidate key
			}
			
			if(checkForThird(lhs, rhs, cks)) {
				isBMinusA = true;
			}
			check = isTrivial || isASuper || isBMinusA;
			if(!check) {
				return false;
			}
		}
		return true;
		
	}
	private static ConcurrentHashMap<Set<String>, Set<String>>
	computeDecomposition(ConcurrentHashMap<Set<String>, 
			Set<String>> fds, List<Set<String>> cks){
		boolean ifCandidatePresent = false;
		Set<String> candidate= cks.get(0);
		ConcurrentHashMap<Set<String>, Set<String>> result = new ConcurrentHashMap<>();
		for(Set<String> lhs: fds.keySet()) {
			Set<String>	rhs = fds.get(lhs);
			Set<String> relation = new HashSet<>();
			relation = Sets.union(lhs, rhs);
			result.put(relation, relation);
			if(!ifCandidatePresent) {
				ifCandidatePresent = checkForCandidate(relation, cks);
			}
		}
		if(!ifCandidatePresent) {
			result.put(candidate, candidate);
		}	
		return result;
		
		
	}
	private static boolean checkForCandidate(Set<String> relation, List<Set<String>> cks) {
		for(Set<String> ck: cks) {
			if(relation.containsAll(ck)) {
				return true;
			}
		}
		
		return false;
		
	}
	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String cksStr = args[2];
		final String outputFile = args[3];
		

		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// This stores the functional dependencies provided as input.
		Set<Object> fds = new HashSet<>();
		//ck set
	
		// This stores the candidate keys provided as input.
		List<Set<String>> cks = new ArrayList<>();
		// This stores the final 3NF decomposition, i.e., the output.
		List<Set<String>> decomposition = new ArrayList<>();
		
		
		// TODO 0: Your code here!
		//
		// Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
		//
		int startIndex = relation.indexOf("(");
		int lastIndex = relation.lastIndexOf(")");
		
		String[] split = relation.substring(startIndex + 1, lastIndex).split(",");
		
		for(String str: split)
		// Parse attributes.
			attributes.add(str.trim());
		
		
		// Parse the input functional dependencies. These are already a canonical cover. Recall that attributes can be formed by multiple letters.
		//
		String[] fdSplit = fdsStr.split(";");
		for(String fd:fdSplit) {
			fds.add(fd.trim());
		}

		//fds into HashMap
		ConcurrentHashMap<Set<String>, Set<String>> fdsMap = new ConcurrentHashMap<>();
		fdsMap = fdsIntoMap(fds);
		
		// Parse the input candidate keys. Recall that attributes can be formed by multiple letters.
		String[] ckSplit = cksStr.split(";");
		
		for(String ck:ckSplit) {
			Set<String> cksSet = new HashSet<>();
			for(String set: ck.split(",")) {
				cksSet.add(set.trim());
			}
				
			
			cks.add(cksSet);
//			cksSet = null;
		}
		
		
		// Analyze whether the relation is already in 3NF:
				//	alreadyIn3NF=true
		boolean ifTnf = checkForTnf(fdsMap, cks);
		
		// Decompose FDs in relations:
		//	For each FD A->B:
		//		Create a new relation ri(A union B) and add it to the decomposition
		PrintWriter writer = new PrintWriter(new File(outputFile));
		if(!ifTnf) {
			ConcurrentHashMap<Set<String>, Set<String>> decompositionMap = computeDecomposition(fdsMap, cks);
			
			
			//check for redundancy
			for(Set<String> rx: decompositionMap.keySet()) {
				for(Set<String> ry: decompositionMap.keySet()) {
					if(!rx.equals(ry)) {
						if(ry.containsAll(rx)) {
							decompositionMap.remove(rx);
						}
					}
					
				}
			}
			//Writing into file
			
			for (Set<String> r : decompositionMap.keySet())
				writer.println("r(" + r.stream().sorted().collect(java.util.stream.Collectors.toList()).
						toString().replace("[", "").replace("]", "") + ")");

		}else {
			decomposition.add(attributes);
			for (Set<String> r : decomposition)
				writer.println("r(" + r.stream().sorted().collect(java.util.stream.Collectors.toList()).
						toString().replace("[", "").replace("]", "") + ")");

		}
		writer.close();
		
	}

}
