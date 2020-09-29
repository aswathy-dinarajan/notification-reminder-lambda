package notificationreminder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathResult;
import com.amazonaws.services.simplesystemsmanagement.model.Parameter;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;



public class ReadingNotificationConfig implements RequestHandler<Object, Object> {
	Gson gson = new GsonBuilder().setPrettyPrinting().create();
	String url = "";
	String username = "";
	String password = "";
	Connection conn;
	AmazonSNS snsClient = AmazonSNSClient.builder().build();
	Map<String, MessageAttributeValue> smsAttributes = new HashMap<String, MessageAttributeValue>();
    private String from = "youremail";
	
    public Object handleRequest(Object input,Context context) {
		LambdaLogger logger = context.getLogger();
		try {   
			smsAttributes.put("AWS.SNS.SMS.SenderID", new MessageAttributeValue().withStringValue("+971XXXXXXXXX").withDataType("String"));
			getAWSSSMParameter(logger);
			conn = DriverManager.getConnection(url, username, password);
			Statement stmt = conn.createStatement();
			ResultSet resultSet = stmt.executeQuery("select id,name,message,mobile_no,email,subject from notification_config where month(dob)=month(sysdate()) and day(dob)=day(sysdate());");
			while (resultSet.next()) {
				//sendSMSMessage(resultSet.getString("message"), "+"+resultSet.getString("mobile_no"),logger); for sending SMSs	
				sendEmail(resultSet.getString("message"), resultSet.getString("email"), resultSet.getString("subject"), logger);
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
	    	}
	    }
	    
	}
	
	
	/*private void sendMessagesSQS(List<SendMessageBatchRequestEntry> messages){
		AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
		String queueUrl = sqs.getQueueUrl("firstqueue").getQueueUrl();
		SendMessageBatchRequest send_batch_request = new SendMessageBatchRequest()
		        .withQueueUrl(queueUrl)
		        .withEntries(messages);
		sqs.sendMessageBatch(send_batch_request);

	}*/
	
	private  void sendSMSMessage( String message, String phoneNumber,LambdaLogger logger) {
	        PublishResult result = snsClient.publish(new PublishRequest()
	        		
	                        .withMessage(message)
	                        .withPhoneNumber(phoneNumber)
	                        .withMessageAttributes(smsAttributes));
	        logger.log("MessageId:"+result.getMessageId()); // Prints the message ID.
	}
	
	private void sendEmail(String message,String toEmail,String subject,LambdaLogger logger){
		AmazonSimpleEmailService client = AmazonSimpleEmailServiceClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		SendEmailRequest request = new SendEmailRequest()
				.withDestination(
						new Destination().withToAddresses(toEmail))
				.withMessage(new Message()
						.withSubject(new Content().withCharset("UTF-8").withData(subject))
						.withBody(new Body()
				                  .withHtml(new Content()
				                      .withCharset("UTF-8").withData(message))))
				.withSource(from);
		client.sendEmail(request);
		System.out.println("Email sent!");
	}
	
}
