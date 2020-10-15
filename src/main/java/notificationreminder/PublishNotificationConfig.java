package notificationreminder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathResult;
import com.amazonaws.services.simplesystemsmanagement.model.Parameter;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * If the AWS account is in sandbox mode both the sender and receiver emails should be verified in SES service. Once it is moved to production mode mails can be sent to unverified recipients
 * In case of SMS depends on the countries the mobile number belongs the process of registering SenderIds varies.
 * @author Aswathy
 *
 */

public class PublishNotificationConfig implements RequestHandler<Object, Object> {
	Gson gson = new GsonBuilder().setPrettyPrinting().create();
	String url = "";
	String username = "";
	String password = "";
	String snstopic = "";
	Connection conn;
	AmazonSNS client = AmazonSNSClientBuilder.defaultClient();
	
    public Object handleRequest(Object input,Context context) {
		LambdaLogger logger = context.getLogger();
		try {   
			getAWSSSMParameter(logger);
			conn = DriverManager.getConnection(url, username, password);
			Statement stmt = conn.createStatement();
			ResultSet resultSet = stmt.executeQuery("select id,name,message,mobile_no,email,subject from notification_config where month(dob)=month(sysdate()) and day(dob)=day(sysdate());");
			StringBuilder sb = new StringBuilder();
			while (resultSet.next()) {
				sb = new StringBuilder();
				sb.append(resultSet.getString("message")).append("#").append(resultSet.getString("email")).append("#").append(resultSet.getString("subject")).append("#").append(resultSet.getString("mobile_no"));
				sendMessagesSNS(logger,sb.toString());
			}
			logger.log("Successfully executed query");

		} catch (Exception e) {
			e.printStackTrace();
			logger.log("Caught exception: " + e.getMessage());
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	private void getAWSSSMParameter(LambdaLogger logger){
		AWSSimpleSystemsManagement client = AWSSimpleSystemsManagementClientBuilder.standard().build();
	    GetParametersByPathRequest request = new GetParametersByPathRequest();
	    request.setWithDecryption(true);
	    request.setPath("/config/notification/");
	    GetParametersByPathResult result = client.getParametersByPath(request);
	    List<Parameter> parameters = result.getParameters();
	    if(parameters != null && !parameters.isEmpty()){
	    	for(Parameter p : parameters){
	    		if("/config/notification/spring.datasource.url".equals(p.getName())){
	    			url = p.getValue();
	    		}
	    		if("/config/notification/spring.datasource.password".equals(p.getName())){
	    			password = p.getValue();
	    		}
	    		if("/config/notification/spring.datasource.username".equals(p.getName())){
	    			username = p.getValue();
	    		}
	    		if("/config/notification/sns.topic.name".equals(p.getName())){
	    			snstopic = p.getValue();
	    		}
	    	}
	    }
	    
	}
	
	
	private void sendMessagesSNS(LambdaLogger logger,String message){
		final PublishRequest publishRequest = new PublishRequest(snstopic, message);
	    client.publish(publishRequest);
		logger.log(" publishing message ");

	}
	
	
}
