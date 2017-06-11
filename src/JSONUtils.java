

import java.io.InputStream;
import java.util.Scanner;

import org.json.FileHandle;
import org.json.JSONObject;

public class JSONUtils {
	
	// Read json string
	public static  String getJSONStringFromFile(String path)
	{
		InputStream in = FileHandle.inputStreamFromFile(path);
		Scanner src = new Scanner(in) ;
		
		String json = src.useDelimiter("\\Z").next();
		src.close();
		
		return json;
		
	}
	
	// Convert json string to json object
	public static  JSONObject getJSONObjectFromFile(String path)
	{
		return new JSONObject(getJSONStringFromFile(path)); 
	}
	
	// Check whether JSON object exists
	public static boolean JSONObjectExists(JSONObject jsonObject, String key)
	{
		Object o;
		try
		{
			System.out.println("Object Exists");
		}
		catch(Exception er)
		{
			return false;
		
		}
		//return o != null;
		return true;
		
	}
	
}
