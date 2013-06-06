import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class Driver {
	private static AmazonSQS sqsClient;
	private static Scanner scn = new Scanner(System.in);
	private static final String LINE_SEPARATOR = "------------------";
	private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/875425895862/ContactManagerQueue";
	private static final String ITEM_ID_KEY = "itemId";
	private static final String FIRST_KEY = "first";
	private static final String LAST_KEY = "last";
	private static final String URL_KEY = "url";
	
	public static void main(String[] args) {
		System.out.println("Welcome to the Contact Manager SQS Polling Formatter");
		System.out.println(LINE_SEPARATOR);
		System.out.println("Press enter to poll our queue and process any messages...");
		
		sqsClient = getSQSClient();
		
		processMessages();

	}


	/********************************************************************
	* Get an SQS client using the user's credentials
	*********************************************************************/
	public static AmazonSQS getSQSClient() {
		AWSCredentials myCredentials;
		AmazonSQS sqsClient = null;
		
		try {
			//get credentials from environment variables
			//myCredentials = new DefaultAWSCredentialsProviderChain().getCredentials();
			myCredentials = new EnvironmentVariableCredentialsProvider().getCredentials();
			sqsClient = new AmazonSQSClient(myCredentials); 
		} catch (Exception ex) {
			System.out.println("There was a problem reading your credentials.");
			System.out.println("Please make sure you have updated your environment variables with your AWS credentials and restart.");
			System.exit(0);
		}
		
		return sqsClient;
	}
	
	
	private static void processMessages() {
		ReceiveMessageResult rslts = sqsClient.receiveMessage(new ReceiveMessageRequest().withQueueUrl(QUEUE_URL));
		if (rslts.getMessages().size() > 0) {
			for (Message msg : rslts.getMessages()) {
				performContactOperations(msg);
			}
		} else {
			System.out.println("No messages to process. Try again later.");
		}
	}


	private static void performContactOperations(Message msg) {
		Map<String, String> contactInfo = getContactInfoFromMessage(msg);
		if (createContactPageInS3(contactInfo) && sendNotification(contactInfo)) {
			removeMessageFromQueue(msg);
		} else {
			System.out.println("There was a problem processing msg " + msg.getMessageId());
		}
	}
	
	private static boolean sendNotification(Map<String, String> contactInfo) {
		String itemId = contactInfo.get(ITEM_ID_KEY);
		String first = contactInfo.get(FIRST_KEY);
		String last = contactInfo.get(LAST_KEY);
		String url = contactInfo.get(URL_KEY);
		
		if (itemId == null || first == null || last == null || url == null) {
			System.out.println("Could not create SNS message due to missing input(s)");
			return false;
		} else {
			return true;
		}
	}


	private static void removeMessageFromQueue(Message msg) {
		sqsClient.deleteMessage(new DeleteMessageRequest().withQueueUrl(QUEUE_URL).withReceiptHandle(msg.getReceiptHandle()));
	}


	// "itemId" : "3514", "first" : "Sam", "last" : "Henry", "url" : "https://s3.amazonaws.com/cspp51083.samuelh.simplecontacts/SamHenry3514.html"
	private static Map<String,String> getContactInfoFromMessage(Message msg) {
		Map<String,String> contactInfo = new HashMap();
		
		for (Entry<String, String> entry : msg.getAttributes().entrySet()) {
			if (entry.getKey().equals(ITEM_ID_KEY) || entry.getKey().equals(FIRST_KEY) || entry.getKey().equals(LAST_KEY) || entry.getKey().equals(URL_KEY)) {
				contactInfo.put(entry.getKey(), entry.getValue());
			} 
		}
		
		return contactInfo;
	}


	/********************************************************************
	* Create a contact's page in S3
	 * @throws Exception 
	*********************************************************************/
	private static boolean createContactPageInS3(Map<String,String> contactInfo) {
		String url = contactInfo.get(URL_KEY);
		if (url == null) {
			System.out.println("Invalid input. URL must be specified to correlate across the system");
			return false;
		}
		
		String first = contactInfo.get(FIRST_KEY);
		String last = contactInfo.get(LAST_KEY);
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
		try {
			//create the new HTML file
			File contactDocument = new File (url);
			FileWriter fw;
			fw = new FileWriter(contactDocument);
			fw.write(newDocument);
			fw.close();
			
			//store HTML file with public accessibility in S3
			getS3Client().putObject(new PutObjectRequest(s3bucketName, url, contactDocument).withCannedAcl(CannedAccessControlList.PublicRead));
			
			System.out.println("Succesfully added " + url + " to your S3 bucket " + s3bucketName);
			
			//delete the local file after storing in S3 so it is not retained
			contactDocument.delete();

			return true;

		} catch (Exception ex) {
			System.out.println("There was a problem creating a contact page in S3 for contact " + first + " " + last);
			return false;
		}
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
			System.out.println("There was a problem reading your credentials.");
			System.out.println("Please make sure you have updated your environment variables with your AWS credentials and restart.");
			System.exit(0);
		}
		
		return s3client;
	}

}
