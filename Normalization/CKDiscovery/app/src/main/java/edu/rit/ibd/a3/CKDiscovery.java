package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

public class CKDiscovery {
	public static boolean checkForAttributes(Set<String> closure, Set<String> attributes) {
		Object[] closureArr = closure.toArray();
		Object[] attributesArr = attributes.toArray();
		Arrays.sort(closureArr);
		Arrays.sort(attributesArr);

		if (Arrays.equals(closureArr, attributesArr)) {
			return true;
		}
		return false;
	}

	public static Set<String> mergeSet(Set<String> a, Set<String> b) {

		// Adding all elements of respective Sets

		// using addAll() method
		return new HashSet<>() {
			{
				addAll(a);
				addAll(b);
			}
		};
	}

	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String outputFile = args[2];
		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// This stores the functional dependencies provided as input.
		Set<Object> fds = new HashSet<>();
		// This stores the candidate keys discovered; each key is a set of attributes.
		List<Set<String>> keys = new ArrayList<>();

		int startIndex = relation.indexOf("(");
		int lastIndex = relation.lastIndexOf(")");

		String[] split = relation.substring(startIndex + 1, lastIndex).split(",");

		for (String str : split)
			// Parse attributes.
			attributes.add(str.trim());

		// get fds
		String[] getFds = fdsStr.split(";");
		String leftHandSide = "", rightHandSide = "";
		// Parse FDs.
		for (String str : getFds) {

			fds.add(str);
		}

		// Discover candidate keys.
		// split into lhs and rhs

		Set<String> case1 = new HashSet<>(), case2 = new HashSet<>(), case3 = new HashSet<>(), case4 = new HashSet<>();
		ArrayList<String> lhs = new ArrayList<>();
		ArrayList<String> rhs = new ArrayList<>();
		for (Object fd : fds) {

			String[] arr = fd.toString().split("->");
			lhs.add(arr[0]);
			rhs.add(arr[1]);

		}

		boolean isLhs = false, isRhs = false;
		// Split attributes by case.
		for (String attr : attributes) {
			for (String left : lhs) {
				if (left.contains(attr)) {
					isLhs = true;
					break;
				}
			}
			for (String right : rhs) {
				if (right.contains(attr)) {
					isRhs = true;
					break;
				}
			}
			if (isLhs && isRhs) {
				case4.add(attr);
				isLhs = false;
				isRhs = false;
				continue;
			} else if (isLhs && !isRhs) {
				case3.add(attr);
				isLhs = false;
				isRhs = false;
				continue;
			} else if (!isLhs && isRhs) {
				case2.add(attr);
				isLhs = false;
				isRhs = false;
				continue;
			} else if (!isLhs && !isRhs) {
				case1.add(attr);
				isLhs = false;
				isRhs = false;
				continue;
			}
		}

		// Find the core. union of case 1 and case 3
		Set<String> core = new HashSet<>();
		for (String str : case1)
			core.add(str.trim());
		for (String str : case3)
			core.add(str.trim());

		// Compute the closure of the core.
		Set<String> closure = computeClosure(fds, core);
		boolean ifAllAttributes = checkForAttributes(closure, attributes);

//		if (/* If all the attributes are present in the closure. */ closure)
		// Add key.
		if (ifAllAttributes) {
			keys.add(core);
		}

//		else {
		// If not, use Sets.combinations to find all possible combinations of
		// attributes.
//		}
		else {
			boolean currentSizeAllCombination = true;

			for (int size = 1; size < case4.size(); size++) {
				currentSizeAllCombination = true;
				for (Set<String> possibleCandidateKeys : Sets.combinations(case4, size)) {

					Set<String> merged = mergeSet(possibleCandidateKeys, core);
					Set<String> stripedMerged = new HashSet<>();
					for (String str : merged) {
						stripedMerged.add(str.strip());
					}

					Set<String> currentClosure = computeClosure(fds, stripedMerged);
					boolean isCandidate = checkForAttributes(currentClosure, attributes);
					if (isCandidate) {

						boolean isPresent = false;
						for (Set<String> key : keys) {
							if (stripedMerged.containsAll(key))
								isPresent = true;
						}
						if (!isPresent)
							keys.add(stripedMerged);
					} else {
						currentSizeAllCombination = false;
					}

				}
				if (currentSizeAllCombination)
					break;
			}

		}

		// TODO 0: End of your code.

		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Set<String> key : keys)
			writer.println(key.stream().sorted().collect(java.util.stream.Collectors.toList()).toString()
					.replace("[", "").replace("]", ""));

		writer.close();
	}

	private static Set<String> computeClosure(Set<Object> fds, Set<String> set) {
		Set<String> result = new HashSet<>(set);

		Set<String> temp = new HashSet<>(result);
		int resultSizeInitial = result.size();
		int resultSizeLater = 0;

		while (resultSizeInitial != resultSizeLater) {

			resultSizeInitial = result.size();

			for (Object fd : fds) {

				String[] split = fd.toString().split("->");
				Set<String> left = new HashSet<String>();
				for (String s : split[0].split(",")) {
					left.add(s.trim());
				}
				String[] secondSplit = split[1].split(",");

				if (result.containsAll(left)) {

					for (String str : secondSplit) {

						result.add(str.trim());
					}
					;

				}

			}
			resultSizeLater = result.size();

		}

//		System.out.println("closure "+ result);
		return result;
	}

}
