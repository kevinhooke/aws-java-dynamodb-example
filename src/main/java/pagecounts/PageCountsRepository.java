package pagecounts;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;


public class PageCountsRepository {

	private static final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
			.withCredentials(new EnvironmentVariableCredentialsProvider())
			.withRegion(Regions.US_EAST_1)
			.build();
	private static final DynamoDB dynamoDB = new DynamoDB(client);	

	private static final String TABLE_NAME = "webapp_page_count";

	public Integer getPageCount() {
		Table table = dynamoDB.getTable(TABLE_NAME);

		GetItemOutcome outcome = table.getItemOutcome(
                "page_id", 1);
		return outcome.getItem().getInt("pageCount");
	}
	
	/**
	 * Increments pageCount using DynamoDB atomic update
	 * @return
	 */
	public Integer incrementPageCount(int pageId) {
		System.out.println("PageCountsRepository.incrementPageCount() called with pageId: " + pageId);
		Table table = dynamoDB.getTable(TABLE_NAME);

		Map<String,String> attrNames = new HashMap<String,String>();
		attrNames.put("#p", "pageCount");

		Map<String,Object> attrValues = new HashMap<String,Object>();
		attrValues.put(":val", 1);
		        
		UpdateItemOutcome outcome = table.updateItem(
		    "page_id", pageId, 
		    "set #p = #p + :val", 
		    attrNames, 
		    attrValues);
		
		//TODO: UpdateIteOutcome should have result but this doesn't seem to work
		//outcome.getItem().getInt("pageCount");
		return this.getPageCount(); 
	}

}
