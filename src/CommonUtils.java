import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class CommonUtils {

	// This function will calculate customer LTV
	public static float calculateLTV(String customer_id,Connection conn1) throws SQLException
	{
		
		String fetchEventdate;
		String fetchTotalAmount;
		long totalDaysActive;
		
		// fetch minimum and maximum event date from site_visit and order table
		fetchEventdate = "select max(a.event_time) max_time,min(a.event_time) min_time from (  select customerorder.event_time from CUSTOMER ,customerorder   where customer.customer_id = customerorder.customer_id and customer.customer_id ='"+customer_id+"'  union all  select site_visit.event_time from CUSTOMER , SITE_VISIT where customer.customer_id = site_visit.customer_id and customer.customer_id ='"+customer_id+"')a";
		Statement sta = conn1.createStatement();
		ResultSet res = sta.executeQuery(fetchEventdate );
		Date [] cal_date ={new Date(),new Date()}; 
		while (res.next()) {
								cal_date[0] = res.getDate("max_time");
								cal_date[1] = res.getDate("min_time");
							}
		
		
		// Fetch total amount spend by customer 
		fetchTotalAmount  = "select sum(total_amount)total_amount from CUSTOMERORDER where CUSTOMERORDER.customer_id ='"+customer_id+"'";
		Statement sta1 = conn1.createStatement();
		ResultSet res1 = sta1.executeQuery(fetchTotalAmount );
		
		float tot_amount[] = {0}; 
		while (res1.next()) {
			tot_amount[0] = 0;
			tot_amount[0] = res1.getInt("total_amount");
			
		}
		
		float total_amount = tot_amount[0];
		
		// Calculate total no days spend by customer
		long date_diff = cal_date[0].getTime() - cal_date[1].getTime();
		
		// Calculate no of weeks customer is active 
		long DaysActive = (date_diff / (7*24 * 60 * 60 * 1000));
		
		if(DaysActive== 0 )
			totalDaysActive = 1;
		else
			totalDaysActive = DaysActive;
		
		// Calculate average amount spend by customer in a week
		float average_amount = 7 * total_amount/totalDaysActive ;
		
		// Calculate revenue of the customer
		float revenue = (float) (52*(average_amount*totalDaysActive) * 10);
		
		return revenue;
		
		
		
		
	}
	
	
		 
	    	
}
