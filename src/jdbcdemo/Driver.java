package jdbcdemo;

import java.sql.*;

public class Driver {

	public static void main(String[] args) {
		String url = "jdbc:mysql://localhost:8889/EMS";
		String user = "root";
		String password = "root";
		
		try{
			//1. Get connection to database
			Connection myCon = DriverManager.getConnection(url, user, password);
			
			//2. Create a statement
			Statement myStmt = myCon.createStatement();
			
			//3. Execute SQL query
			
			ResultSet myRs = myStmt.executeQuery("select * from RAM");
			
			String sql = "LOAD DATA LOCAL INFILE '/Users/Yaovi/Desktop/Transport/export/RAM.tsv' INTO TABLE RAM ";
			String sql1 = "LOAD DATA LOCAL INFILE '/Users/Yaovi/Desktop/Transport/export/RAM.tsv' INTO TABLE RAM IGNORE 1 LINES;";
			//myStmt.executeUpdate(sql);
			myStmt.executeUpdate(sql1);
			
			//4. Process the result set
//			while(myRs.next()){
				//System.out.println(myRs.getString("last_name") + ", " + myRs.getString("first_name"));
//				System.out.println(myRs.toString());
//			}
			
			
			System.out.println("Insert complete.");
			
		}
		catch(Exception exc){
			exc.printStackTrace();
		}

	}

}
