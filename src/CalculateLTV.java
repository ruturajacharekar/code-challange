/* 
Description File of the Source code for Shutter Fly Coding Challenge

-----------------------------------REQUIREMENTS To EXECUTE THE CODING CHALLENGE----------------------------------------------------------------------------------------------------------
DERBY: I am Apache Derby 10.12.1.1 as my Inmemory Database. The DataBase is created using the sqlcommand file in the "src" folder.
 
ARGUMENT TO THE PROGRAM: An argument needs to be passed, that is the Topx Value needs to put in the java argument window. 

-----------------------------------IMPLEMENTATION OF THE CODING CHALLENGE----------------------------------------------------------------------------------------------------------------

INGENT FUNCTION: The Injest function gets the DATA according to attribute "TYPE" mention in the input JSON file.

DATA : Data is Stored USING IN-MEMORY DERBY DataBase. IT IS ACCESSED USING THE JSON STRING in JSONUtils.

CALCULATE LTV: WHEN THE NEW CUSTOMER IS INTO THE SYSTEM IT WILL CALUCATE LTV VALUSE OF THE CUSTOMER AND PASS THAT VALUE AND STORE THAT VALUE IN HASH-MAP MAP FOR FURTHER PROSESSING.

TOP X FUNCTION: IT WILL RETURN THE REVENEW OF TOP "X" CUSTOMERS GIVEN THROUGH THE ARGUMENT

-----------------------------------PERFORMANCE OF THE IMPLEMENTATION--------------------------------------------------------------------------------------------------------------------- 

1: THE TOTAL NUMBER OF DAYS THE CUSTOMER WAS ACTIVE IS CALCULATE BY USING BOTH THE TABLES BECAUSE IT CAN BE POSSIBLE THAT CUSTOMER VISITED THE SITE FIRST AND PLACE THE ORDER LATER.

2: TO STORE THE CUSTOMERS REVENEW EFFICIENTLY I HAVE USED HASH MAP.

-----------------------------------IMPROVISING THE IMPLEMENTATION----------------------------------------------------------------------------------------------------------------------- 
IN CURRENT PROGRAM, JSON  THERE IS DIRECT INSERT STATEMENT FOR STORING THE DATA  IN DATA BASE WHICH CAN BE USED TO IMPROVE BY USING JAVA FRAME WORK

ALTERNATE WAY OF IMPLEMENTATION TO ACCESS THE DATA CAN BE USING THE DATA ACCESS OBJECT LAYER.

*/


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

public class CalculateLTV {

		private static Connection conn1 = null;

		public static void main(String[] args) throws SQLException {
		
			try{
				
				// Create database connection with derby
			    String dbURL1 = "jdbc:derby://localhost:1527/LTA;create=true;user=root;password=root";
	            final Connection conn = DriverManager.getConnection(dbURL1);
	            conn1 = conn;	

	            Float customerRevenue;
				
	            // noofLTA is no of records which we want to display
				Integer noofLTA = Integer.parseInt(args[0]); 
				
				// Hash Map to store customer wise revenue
				Map<String, Float> perCustomerRevenue = new HashMap<String, Float>();
				
				// read json data from input file
				final JSONObject obj = JSONUtils.getJSONObjectFromFile("/inputfile.json");
				
				//Read json array from input data
				final JSONArray inputdata = obj.getJSONArray("input");
				
				final int n = inputdata.length();
				
				//Call to Ingest method
				for (int i = 0; i < n; ++i) {
					   JSONObject currentdata = inputdata.getJSONObject(i);
					   ingest(currentdata.getString("type"),currentdata);
				   }
				
				// loop to calculate and store customer wise revenue
				for (int i = 0; i < n; ++i)
				{
					JSONObject currentdata = inputdata.getJSONObject(i);
					
					if(currentdata.getString("type").equals("CUSTOMER") && currentdata.getString("verb").equals("NEW"))
				      {
				    	  customerRevenue= CommonUtils.calculateLTV(currentdata.getString("key"), conn1);
				    	  perCustomerRevenue.put(currentdata.getString("key"), customerRevenue);
				      }
					else
						continue;
				}
				
				// Call to calculate top x records
				TopXSimpleLTVCustomers(noofLTA,perCustomerRevenue);
				conn1.close();
				System.out.println("Program completed successfully");
				return;
				
			}
			catch(Exception er)
			{
				er.printStackTrace();
				conn1.close();
			}		
		 	/* Statement sta = conn1.createStatement();
	            ResultSet res = sta.executeQuery(
	                    "SELECT * FROM customer");
	            
	            
	            while (res.next()) {
					                int id = res.getInt("customer_id");
					                String name = res.getString("type");
					                String job = res.getString("verb");
					                System.out.println(id+"   "+name+"    "+job);
	            					}
	            					
	            					
	          */  					
		 

	}



	private static void TopXSimpleLTVCustomers(Integer noofLTA,Map<String, Float> perCustomerRevenue) {
		/*
		 input parameter 1. no of records to display
		 				 2.	Customer wise revenue
		 */
		int total_records;
		String outputStr ="";
		// Logic to sort hashmap
		
		Set<Entry<String,Float>> set = perCustomerRevenue.entrySet();
		
		List<Entry<String,Float>> list = new ArrayList<Entry<String,Float>>(set);
		
		Collections.sort(list,new Comparator<Entry<String,Float>>()
				{

					public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
						return o2.getValue().compareTo(o1.getValue());
					}
			
				}
		
				);
		
		
		// logic to display top x records
		if(noofLTA>list.size())
			total_records =  list.size();
		else
			total_records =  noofLTA;	
		
		System.out.println("Top 10 Customer and their revenue is as follows :: ");
		for(int i=0;i<total_records;i++)
				{
					
					System.out.println(list.get(i));
					outputStr= outputStr +" \n "+list.get(i);
				}	
		// logic to write result in output file
		WriteOutputToFile(outputStr);
		
		
	}



	private static void WriteOutputToFile(String outputStr) {
		/*
		 * input parameter : 1. String to be write in output folder
		 */
		FileOutputStream revenueoutput = null;
		File file;
		
		// location of output file
		file = new File("d:/outputfile.txt");
		
		try {
			revenueoutput = new FileOutputStream(file);
			
			// logic to create output file
			if (!file.exists()) {
				try {
					file.createNewFile();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			// logic to write output in output file
			byte[] contentInBytes = outputStr.getBytes();
				revenueoutput.write(contentInBytes);
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}



	private static void ingest(String input_type, JSONObject object2) throws SQLException {
		
		/*
		 input parameter 1. Event
		                 2. JSON object to insert 
		 */
		Statement stmt = conn1.createStatement();
		
		// logic to insert update records in database
		
		if(input_type.equals("CUSTOMER"))
		{
			if(object2.getString("verb").equals("NEW"))
            stmt.execute("insert into customer values ('" +
           	object2.getString("type") + "','" + object2.getString("verb") + "','" + object2.getString("key")+"','" + object2.getString("event_time")+"','" + object2.getString("last_name")+"','" + object2.getString("adr_city")+"','" + object2.getString("adr_state") +"')");
			else
				stmt.execute("UPDATE customer SET type = '"+object2.getString("type")+"', verb = '"+object2.getString("verb")+"', event_time = '"+object2.getString("event_time")+"' , last_name = '"+object2.getString("last_name")+"', adr_city = '"+object2.getString("adr_city")+"', adr_state = '"+object2.getString("adr_state")+"' WHERE customer_id = '"+object2.getString("key")+"'");
				
		}
		else if(input_type.equals("SITE_VISIT"))
		{
			stmt.execute("insert into site_visit values ('" +
       	object2.getString("type") + "','" + object2.getString("verb") + "','" + object2.getString("key")+"','" + object2.getString("event_time")+"','" + object2.getString("customer_id")+"')");
		}
		else if(input_type.equals("IMAGE"))
		{
			stmt.execute("insert into image values ('" +
           	object2.getString("type") + "','" + object2.getString("verb") + "','" + object2.getString("key")+"','" + object2.getString("event_time")+"','" + object2.getString("camera_make")+"','" + object2.getString("camera_model") +"','" + object2.getString("customer_id")+"')");
		}
		else if(input_type.equals("ORDER"))
		{
			stmt.execute("insert into customerorder values ('" +
           	object2.getString("type") + "','" + object2.getString("verb") + "','" + object2.getString("key")+"','" + object2.getString("event_time")+"'," + object2.getNumber("total_amount")+",'" + object2.getString("customer_id")+"')");
		}
		else
		{
			System.out.println("Incorrect type");
		}	
		stmt.close();
		
	}


}
