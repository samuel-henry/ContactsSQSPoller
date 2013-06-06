import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Driver {
	private static AmazonSQS sqsClient;
	private static AmazonSNS snsClient;
	private static AmazonS3 s3Client;
	private static final String LINE_SEPARATOR = "------------------";
	private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/875425895862/ContactManagerQueue";
	private static final String FIRST_KEY = "first";
	private static final String LAST_KEY = "last";
	private static final String URL_KEY = "url";
	private static final String UPDATE_TOPIC_ARN = "arn:aws:sns:us-east-1:875425895862:51083-updated";
	
	public static void main(String[] args) {
		System.out.println("Welcome to the Contact Manager SQS Polling Formatter");
		System.out.println(LINE_SEPARATOR);
		
		sqsClient = getSQSClient();
		snsClient = getSNSClient();
		processMessages();

	}


	/********************************************************************
	* Get SNS client using the user's credentials
	*********************************************************************/
	private static AmazonSNS getSNSClient() {
		AWSCredentials myCredentials;
		AmazonSNS snsClient = null;

		try {
			//get credentials from default provider chain
			myCredentials = new EnvironmentVariableCredentialsProvider().getCredentials();
			snsClient = new AmazonSNSClient(myCredentials);
		} catch (Exception ex) {
			System.out.println("There was a problem reading your credentials.");
			System.exit(0);
		}
		return snsClient;
	}

	/********************************************************************
	* Get an SQS client using the user's credentials
	*********************************************************************/
	public static AmazonSQS getSQSClient() {
		AWSCredentials myCredentials;
		AmazonSQS sqsClient = null;
		
		try {
			//get credentials from environment variables
			myCredentials = new DefaultAWSCredentialsProviderChain().getCredentials();
			sqsClient = new AmazonSQSClient(myCredentials); 
		} catch (Exception ex) {
			System.out.println("There was a problem reading your credentials.");
			System.out.println("Please make sure you have updated your environment variables with your AWS credentials and restart.");
			System.exit(0);
		}
		
		return sqsClient;
	}
	
	/********************************************************************
	* Get an S3 client using the user's credentials
	*********************************************************************/
	private static AmazonS3 getS3Client() {
		AWSCredentials myCredentials;
		AmazonS3 s3client = null;
		
		try {
			//get credentials from environment variables
			myCredentials = new DefaultAWSCredentialsProviderChain().getCredentials();
			s3client = new AmazonS3Client(myCredentials); 
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("There was a problem reading your credentials.");
			System.out.println("Please make sure you have updated your environment variables with your AWS credentials and restart.");
			System.exit(0);
		}
		
		return s3client;
	}
	
	/********************************************************************
	* Retrieve messages available in the queue and initiate processing
	*********************************************************************/
	private static void processMessages() {
		ReceiveMessageResult rslts = sqsClient.receiveMessage(new ReceiveMessageRequest().withQueueUrl(QUEUE_URL));
		
		if (rslts.getMessages().size() > 0) {
			for (Message msg : rslts.getMessages()) {
				performContactOperations(msg);
				System.out.println("Processed message " + msg.getMessageId());
			}
		} else {
			System.out.println("No messages to process. Try again later.");
		}
	}

	/********************************************************************
	* Perform the necessary operations given the unit of work:
	* 1. Create the S3 page
	* 2. Send the update notification
	* 3. Remove the message from the queue
	*********************************************************************/
	private static void performContactOperations(Message msg) {
		
		try {
			JSONObject contactInfo = getContactInfoFromMessage(msg);
			if (createContactPageInS3(contactInfo) && sendNotification(msg.getBody())) {
				removeMessageFromQueue(msg);
			} else {
				System.out.println("There was a problem processing msg " + msg.getMessageId());
			}
		} catch (JSONException e) {
			e.printStackTrace();
			System.out.println(e.toString());
			System.out.println(e.getMessage());
			System.out.println("There was a problem processing msg " + msg.getMessageId());
		}
	}
	
	/********************************************************************
	* Publish an SNS message signifying the creation of our contact
	*********************************************************************/
	private static boolean sendNotification(String msgBody) {
		snsClient.publish(new PublishRequest(UPDATE_TOPIC_ARN, msgBody));
		return true;
	
	}

	/********************************************************************
	* Remove a SQS message from our queue
	*********************************************************************/
	private static void removeMessageFromQueue(Message msg) {
		sqsClient.deleteMessage(new DeleteMessageRequest().withQueueUrl(QUEUE_URL).withReceiptHandle(msg.getReceiptHandle()));
	}

	/********************************************************************
	* Get a JSON representation of our contact's info from the message
	*********************************************************************/
	private static JSONObject getContactInfoFromMessage(Message msg) throws JSONException {
		return new JSONObject(msg.getBody());
	}


	/********************************************************************
	* Create a contact's page in S3
	 * @throws JSONException 
	 * @throws Exception 
	*********************************************************************/
	private static boolean createContactPageInS3(JSONObject contactInfo) {
		String first = "",
				last = "";
		
		try {
			String url = (String) contactInfo.get(URL_KEY);
			if (url == null) {
				System.out.println("Invalid input. URL must be specified to correlate across the system");
				return false;
			}
			
			first = (String) contactInfo.get(FIRST_KEY);
			last = (String) contactInfo.get(LAST_KEY);
			String htmlTemplateBeginning = "<!DOCTYPE html><html><body><table>";
			String htmlTemplateEnding = "</body></html>";
			String htmlHeaderRow = "<tr>";
			String htmlDetailRow = "<tr>";
	
			//build the document
			//add a table cell for the first name
			if (first != null && first.length() > 0) {
				htmlHeaderRow = htmlHeaderRow + "<th>" + FIRST_KEY + "</th>";
				htmlDetailRow = htmlDetailRow + "<td>" + first + "</td>";
			} 
			
			//add a table cell for the last name if it was entered
			if (last != null && last.length() > 0) {
				htmlHeaderRow = htmlHeaderRow + "<th>" + LAST_KEY + "</th>";
				htmlDetailRow = htmlDetailRow + "<td>" + last + "</td>";
			}
			
			//terminate the rows
			htmlHeaderRow = htmlHeaderRow + "</tr>";
			htmlDetailRow = htmlDetailRow + "</tr>";
			
			String newDocument = htmlTemplateBeginning + htmlHeaderRow + htmlDetailRow + htmlTemplateEnding;
			
			String s3bucketName = "cspp51083.samuelh.simplecontacts";
			//create the new HTML file
			File contactDocument = new File (url.substring(58));
			FileWriter fw;
			fw = new FileWriter(contactDocument);
			fw.write(newDocument);
			fw.close();
			//get the S3 client
			s3Client = getS3Client();
			
			//store the HTML file in S3
			s3Client.putObject(new PutObjectRequest(s3bucketName, url.substring(58), contactDocument).withCannedAcl(CannedAccessControlList.PublicRead));
			
			//delete the local file after storing in S3 so it is not retained
			contactDocument.delete();

			return true;

		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(ex.toString());
			System.out.println(ex.getMessage());
			System.out.println("There was a problem creating a contact page in S3 for contact " + first + " " + last);
			return false;
		}
	}
	
}
