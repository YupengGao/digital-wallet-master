// example of program that detects suspicious transactions
// fraud detection algorithm
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.io.File;

public class antifraud {

	//read the cleaned data and build the undirected graph by using a hashmap
	//key present the node, value represent the directly adjacent node
	public static void buildGraph(HashMap<Integer, HashSet<Integer>> map, String cleanedTrain) throws IOException{
		String input = cleanedTrain;
		try{
			BufferedReader brfferedReader = new BufferedReader(new FileReader(input));
			String line = "";
            while ((line = brfferedReader.readLine()) != null) {
            	String[] field = line.split(" ");
                if(field.length != 2){
                	continue;
                }
                int number1 = Integer.parseInt(field[0]);// read the cleaned data
            	int number2 = Integer.parseInt(field[1]);
            	if(map.containsKey(number1)){//use hashmap to represent the undirected graph
                	map.get(number1).add(number2);
                }else{
                	HashSet<Integer> set = new HashSet<Integer>();
                	set.add(number2);
                	map.put(number1, set);
                }
                if(map.containsKey(number2)){
                	map.get(number2).add(number1);
                }else{
                	HashSet<Integer> set = new HashSet<Integer>();
                	set.add(number1);
                	map.put(number2, set);
                }
            }
            brfferedReader.close();
            
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	//clean both batch_payment and stream_payment.csv file to txt file which only contain the two id
	public static void cleanData(String trainingData, String testingData, String cleanedTrain, String cleanedTest) throws IOException{
		
		
		String cleanedTrainPath = cleanedTrain;
		String cleanedTestPath = cleanedTest;
		try{
			BufferedReader brfferedReader = new BufferedReader(new FileReader(trainingData));
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(cleanedTrainPath));
			String line = "";
			int n = 0;	
            while ((line = brfferedReader.readLine()) != null) {
            	if(n == 0) {
            		n++;
            		continue;//skip the first header line;
	            	
            	}
                // use comma as separator
            	// data clean
                String[] field = line.split(",");
                if(field.length != 5){
                	continue;
                }
                String number1 = field[1].replaceAll("[^0-9]", "");//we need to see the two numbers are valid
                String number2 = field[2].replaceAll("[^0-9]", "");
                bufferedWriter.write(number1 + " " + number2 + "\n");
            }
            brfferedReader.close();
            bufferedWriter.close();
            brfferedReader = new BufferedReader(new FileReader(testingData));
			bufferedWriter = new BufferedWriter(new FileWriter(cleanedTestPath));
			line = "";
			n = 0;
			while ((line = brfferedReader.readLine()) != null) {
            	if(n == 0) {
            		n++;
            		continue;//skip the first header line;
            	}
                String[] field = line.split(",");
                if(field.length != 5){
                	continue;
                }
                String number1 = field[1].replaceAll("[^0-9]", "");
                String number2 = field[2].replaceAll("[^0-9]", "");
                bufferedWriter.write(number1 + " " + number2 + "\n");
            }
			brfferedReader.close();
            bufferedWriter.close();
            
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	
	}

	//compute the adjacent node which is within k degree, for each node in the graph
	public static void pre_computeKdegree(HashMap<Integer, HashSet<Integer>> map, int k){
		try{
			// Create one directory
		    String strDirectoy ="./src/database/adjcentList" + String.valueOf(k) + "degree";
		    boolean success = (new File(strDirectoy)).mkdir(); 
		    for(int key : map.keySet()){
		    	HashSet<Integer> KDegreeList = new HashSet<Integer>();
		    	computeAdjacentList(k, key, KDegreeList, map);//compute the kDegreeList for key
		    	writeDB(strDirectoy ,key, KDegreeList);//write DegreeList to file
		    }

	    }catch (Exception e){//Catch exception if any
	      System.err.println("Error: " + e.getMessage());
	    }  
	}
	
	//compute the adjacent node which is within k degree, for given node key by using bfs
	public static void computeAdjacentList(int degree, int key, HashSet<Integer> KDegreeList, HashMap<Integer, HashSet<Integer>> map){
		if(KDegreeList == null || map == null){
			throw new IllegalArgumentException("computeAdjacentList input is invalid");
		}
		List<Integer> list = new LinkedList<Integer>();
    	HashMap<Integer, Integer> distance = new HashMap<>();
    	list.add(key);
    	distance.put(key, 0);
		boolean outOfRange = false;
		while(!list.isEmpty()){// given a node, I calculate the adjacent node which is less than k degree, put them to result
			int count = list.size();
			for(int i = 0 ; i < count; i++){
				int node = list.remove(0);
				int curLevel = distance.get(node);
				if(curLevel + 1 > degree){//if the distance bigger than k degree, then we should break
					outOfRange = true;
					break;
				}
				for(int neigh : map.get(node)){
					if(!distance.containsKey(neigh)){
						distance.put(neigh, curLevel + 1);
						list.add(neigh);
						KDegreeList.add(neigh);
					}
				}
			}
			if(outOfRange){
				break;
			}
		}
	}
	
	//write the adjacent list to database
	public static void writeDB(String strDirectoy,int key, HashSet<Integer> DegreeList){
		if(DegreeList == null){
			throw new IllegalArgumentException("DegreeList is null");
		}
		String name = String.valueOf(key);
		File file = new File(strDirectoy + "/" + name+ ".txt");
		try {
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
			int n = 0;
			for(int num : DegreeList){
				bufferedWriter.write(num+" ");
				n++;
			}
			bufferedWriter.close();

		}catch (IOException e){
		    e.printStackTrace();
		}
	}
	
	//read the id in test data and query for feature1 feature2 feature3
	public static void query(String testDataPath, String path1, String path2, String path3){
		if(testDataPath == null || testDataPath.length() == 0){
			throw new IllegalArgumentException("query input is invalid");
		}
		String output1 = path1;
		String output2 = path2;
		String output3 = path3;
		String testPath = testDataPath;
		try{
			String strDirectoy ="./paymo_output";
		    boolean success = (new File(strDirectoy)).mkdir();
		    // if (success) {
		    //   System.out.println("Directory: " + strDirectoy + " created");
		    // }  
			BufferedReader brfferedReader = new BufferedReader(new FileReader(testPath));
			BufferedWriter bufferedWriter1 = new BufferedWriter(new FileWriter(output1));
			BufferedWriter bufferedWriter2 = new BufferedWriter(new FileWriter(output2));
			BufferedWriter bufferedWriter3 = new BufferedWriter(new FileWriter(output3));
			String line = "";
            while ((line = brfferedReader.readLine()) != null) {
            	String[] field = line.split(" ");
                if(field.length != 2){
                	continue;
                }
                int number1 = Integer.parseInt(field[0]);
            	int number2 = Integer.parseInt(field[1]);
            	//query for feature1
            	if(FeatureK(number1, number2, 1)){
            		bufferedWriter1.write("trusted");
            		bufferedWriter1.newLine();
            	}else{
            		bufferedWriter1.write("unverified");
            		bufferedWriter1.newLine();
            	}
            	//query for feature2
            	if(FeatureK(number1, number2,2)){
            		bufferedWriter2.write("trusted");
            		bufferedWriter2.newLine();
            	}else{
            		bufferedWriter2.write("unverified");
            		bufferedWriter2.newLine();
            	}
            	//query for feature3
            	if(Feature3(number1, number2)){
            		bufferedWriter3.write("trusted");
            		bufferedWriter3.newLine();
            	}else{
            		bufferedWriter3.write("unverified");
            		bufferedWriter3.newLine();
            	}
            	
            }
            brfferedReader.close();
            bufferedWriter1.close();
            bufferedWriter2.close();
            bufferedWriter3.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	}


	//we look up the database for id1 and id2, get the 2 degree adjacent, 
	//if NumOne2DegreeSet and NumTwo2DegreeSet have sth in common then we know the two are within 4 degree
	public static boolean Feature3(int num1, int num2){
		if(num1 == num2){
			return true;
		}
		String lookUpPathNumOne = "./src/database/adjcentList2degree/" + String.valueOf(num1) + ".txt";
		String lookUpPathNumTwo = "./src/database/adjcentList2degree/" + String.valueOf(num2) + ".txt";
		HashSet<Integer> NumOne2DegreeSet = new HashSet<Integer>();
		HashSet<Integer> NumTwo2DegreeSet = new HashSet<Integer>();
		//look up the DB
		try{
			File file1 = new File(lookUpPathNumOne);
			File file2 = new File(lookUpPathNumTwo);
			if(!file1.exists() || !file2.exists()){
				return false;
			}
			BufferedReader brfferedReader1 = new BufferedReader(new FileReader(file1));
			BufferedReader brfferedReader2 = new BufferedReader(new FileReader(file2));
			String line = "";
            while ((line = brfferedReader1.readLine()) != null) {//read 2 degree adjacent list for node1, put to set
            	String[] field = line.split(" ");
            	for(int i = 0 ; i < field.length; i++){
            		int n = Integer.parseInt(field[i]);
            		NumOne2DegreeSet.add(n);
            	}
            }
            line = "";
            while ((line = brfferedReader2.readLine()) != null) {//read 2 degree adjacent list for node2, put to set
            	String[] field = line.split(" ");
            	for(int i = 0 ; i < field.length; i++){
            		int n = Integer.parseInt(field[i]);
            		NumTwo2DegreeSet.add(n);
            	}
            }
            brfferedReader1.close();
            brfferedReader2.close();
            
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
		// for(int i : NumOne2DegreeSet){
		// 	System.out.print(i + " ");
		// }
		// System.out.println();
		// for(int i : NumTwo2DegreeSet){
		// 	System.out.print(i + " ");
		// }
		for(int num : NumOne2DegreeSet){//check every node in set1
			if(NumTwo2DegreeSet.contains(num)){//if set2 also contains the node then it is within 4 degree
				return true;
			}
			if(num == num2){
				return true;
			}
		}
		return false;
	}
	
	public static boolean FeatureK(int num1, int num2, int k){
		if(num1 < 0 || num2 < 0 || k < 0){
			throw new IllegalArgumentException("invalid input for featureK");
		}
		if(num1 == num2){
			return true;
		}
		int degree = 0;
		if(k == 1){
			degree = 1;
		}
		else if(k == 2){
			degree = 2;
		}
		else if(k == 3){
			throw new IllegalArgumentException("k can't be 3 in this case");
		}
		if(degree == 0){
			throw new IllegalArgumentException("degree can't be zero");
		}
		String lookUpPath = "./src/database/adjcentList"+ degree +"degree/" + String.valueOf(num1) + ".txt";
		HashSet<Integer> oneDegreeList = new HashSet<Integer>();
		oneDegreeList.add(num1);
		//look up the DB
		try{
			File fr = new File(lookUpPath);
			if(!fr.exists()){
				return false;
			}
			BufferedReader brfferedReader = new BufferedReader(new FileReader(fr));
			String line = "";
            while ((line = brfferedReader.readLine()) != null) {
            	String[] field = line.split(" ");
            	for(int i = 0 ; i < field.length; i++){
            		int n = Integer.parseInt(field[i]);
            		oneDegreeList.add(n);
            	}
            }
            brfferedReader.close();
            
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
		if(oneDegreeList.contains(num2)){
			return true;
		}
		return false;
	}
	
	
	
	public static void main(String args[]) throws IOException{
		if(args.length != 5){
			throw new IllegalArgumentException("input size should be 5");
		}
		String trainingDataPath = args[0];
		String testingDataPath = args[1];
	    String cleanedTrainPath = "./src/database/cleanedTrain.txt";
	    String cleanedTestPath = "./src/database/cleanedTest.txt";
		String outputData1Path = args[2];
		String outputData2Path = args[3];
		String outputData3Path = args[4];
		String strDirectoy = "./src/database";
		boolean success = (new File(strDirectoy)).mkdir();

        HashMap<Integer, HashSet<Integer>> map = new HashMap<Integer, HashSet<Integer>>();
        HashMap<Integer, HashSet<Integer>> mapDegreeTwo = new HashMap<Integer, HashSet<Integer>>();
        HashMap<Integer, HashSet<Integer>> mapDegreeThree = new HashMap<Integer, HashSet<Integer>>();
        HashMap<Integer, HashSet<Integer>> mapDegreeFour = new HashMap<Integer, HashSet<Integer>>();
        //clean both training data and testing data and write data on cleanedTrainPath.txt and cleanedTest.txt
        cleanData(trainingDataPath, testingDataPath, cleanedTrainPath, cleanedTestPath);
        System.out.println("clean data finished");
        
        //use hashmap to build the graph load into memory
        buildGraph(map, cleanedTrainPath);
        System.out.println("build graph finished");
        
       //pre-compute the adjacentList1degree and log the adjacentList1degree
        pre_computeKdegree(map, 1);
        System.out.println("pre_compute 1degree adjacent list finished");
        
        //pre-compute the adjacentList2degree and log the adjacentList2degree
        pre_computeKdegree(map, 2);
        System.out.println("pre_compute 2degree adjacent list finished");
        
        
        //test the result
        System.out.println("begin to query");
        query(cleanedTestPath,outputData1Path,outputData2Path,outputData3Path);
	}	
}
	
