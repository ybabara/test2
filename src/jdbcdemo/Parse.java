package jdbcdemo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * The program converts TSV file to multiple CSV files
 * 
 *  
 * Following are assumed 
 * 1. Each header section to be written to a file starts with "#Type" 
 * 2. There is an empty line after each section
 * 3. Header information will be ignored
 * 4. Existing files (with identical file name) will be overwritten
 *  
 * @author jvarghes
 * 
 * Revised by ybabara 2/3/2015
 *
 */

public class Parse {
	public static void main(String[] args) {
		String currentLine = null;
		boolean start = false;
		PrintWriter out = null;
		Scanner scanner = null;
		// File inputFile = new File(args[0]);
		File inputFile = new File("/Users/Yaovi/Desktop/dnaexport.tsv");	// Replace with your file name.
		ArrayList<String> TableName = new ArrayList<String>();
		try {
			scanner = new Scanner(inputFile);
			while (scanner.hasNext()) {
				currentLine = scanner.nextLine();
				if (currentLine.startsWith("#Type") && !start) { // starts a new file
					currentLine.replaceAll("#", "Number");
					if (out != null) {
						out.close();
						out = null;
					}

					String dataLine = scanner.nextLine();
					String[] vars = dataLine.split("\t");
					String fileName1 = "_NONAME";
					String fileName2 = null;
					if (vars.length > 0 && !vars[0].isEmpty()) {
						fileName1 = vars[0];
						fileName1 = fileName1.replaceAll(":","_");
						fileName1 = fileName1.replaceAll("-","_");
						fileName2 = fileName1.replaceAll(" ","_");
						TableName.add(fileName2);
					}
					out = new PrintWriter(new BufferedWriter(new FileWriter("/Users/Yaovi/Desktop/Transport/export/"+fileName2+".tsv", false)));
					System.out.println("New TSV File: " + fileName2);
					out.println(currentLine);
					out.println(dataLine);
					start = true;
				} else if (start && currentLine.length() > 0) { // write data to file
					out.println(currentLine);
					//out.println(currentLine.replaceAll("\t", ","));
				} else if (start && currentLine.length() == 0) { // close the file
					out.close();
					start = false;
				} else {
					//System.out.println("HEADER: " + currentLine);
				}
			}
		} catch (Exception e) {
			// e.printStackTrace();
			System.out.println(e.getMessage());
		} finally {
			if (out != null) {
				out.close();
				out = null;
			}
			if (scanner != null) {
				scanner.close();
			}
		}

		System.out.println(TableName);
		System.out.println();
		
		String url = "jdbc:mysql://localhost:8889/Transport";
		String user = "root";
		String password = "root";
		
		try{
			//1. Get connection to database
			Connection myCon = DriverManager.getConnection(url, user, password);
			
			//2. Create a statement
			Statement myStmt = myCon.createStatement();
			
			//3. Execute SQL query
			
			//ResultSet myRs = myStmt.executeQuery("select * from RAM");
			
			//String sql = "LOAD DATA LOCAL INFILE '/Users/Yaovi/Desktop/Transport/export/RAM.tsv' INTO TABLE RAM ";
			for(int i = 0; i< TableName.size();i++){
				String sql = "LOAD DATA LOCAL INFILE '/Users/Yaovi/Desktop/Transport/export/"+ TableName.get(i)+".tsv' "
						+ "INTO TABLE "+TableName.get(i)+ " IGNORE 1 LINES;";
				
				//System.out.println(sql);
				
				System.out.println(TableName.get(i) +" uploaded to database");
				myStmt.executeUpdate(sql);
				
			}
			//String sql1 = "LOAD DATA LOCAL INFILE '/Users/Yaovi/Desktop/Transport/export/"+ TableName.get(i)+".tsv' INTO TABLE RAM IGNORE 1 LINES;";
			//myStmt.executeUpdate(sql);
			//myStmt.executeUpdate(sql1);
			
			//4. Process the result set
//			while(myRs.next()){
				//System.out.println(myRs.getString("last_name") + ", " + myRs.getString("first_name"));
//				System.out.println(myRs.toString());
//			}
			
			
			System.out.println("\nInsert complete.");
			
		}
		catch(Exception exc){
			exc.printStackTrace();
		}
	}

}
