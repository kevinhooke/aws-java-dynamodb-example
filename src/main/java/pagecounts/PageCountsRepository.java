package pagecounts;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;


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

}
