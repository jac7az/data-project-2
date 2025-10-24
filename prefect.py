from prefect import flow, task, get_run_logger
import requests
import boto3
import time

sqs = boto3.client('sqs')  
url='https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/jac7az'

#Task1
@task
def populate_message(url):
    logger=get_run_logger()
    try:
        payload=requests.post(url).json()
        if payload.get('sqs_url'):
            logger.info(f"Got sqs url: {payload.get('sqs_url')}")
            return payload.get('sqs_url')
        else:
            raise ValueError("Link not found.")
    except requests.exceptions.RequestsException as e:
        print(f"Request failure in POST: {e}")
        logger.error("Request failure in POST")
        raise e

#Task2
@task
def get_queue_attributes(url):
    logger=get_run_logger()   
    try:                                                                                     
        response = sqs.get_queue_attributes(QueueUrl=url,AttributeNames=['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible','ApproximateNumberOfMessagesDelayed'])                                      
        attributes=response['Attributes']
        num_messages=int(attributes.get('ApproximateNumberOfMessages',0))
        num_invis=int(attributes.get('ApproximateNumberOfMessagesNotVisible',0))
        num_delay=int(attributes.get('ApproximateNumberOfMessagesDelayed',0))
        if num_messages+num_invis+num_delay==21:
            logger.info(f"All 21 messages received.")
            print(f"Response: {response}")    
            return num_messages
        else:
            logger.error("Error in getting queue messages")
                                                                 
    except Exception as e:                                                                                 
        print(f"Error getting queue attributes: {e}")                                                      
        raise e  
    
@task
def receive_message(url):
    # try to get any messages with message-attributes from SQS queue
    logger=get_run_logger()
    try:
        response = sqs.receive_message(
            QueueUrl=url,
            MessageSystemAttributeNames=['order_no','word'],
            VisibilityTimeout=60,
            WaitTimeSeconds=10
        )
        if response['Messages']==False:
            logger.error("Messages empty")
        
        # print the word:
        word=response['Messages'][0]['MessageAttributes']['word']['StringValue']
        logger.info(f"word received: {word}")
        print(f"MessageAttributes: {response['Messages'][0]['MessageAttributes']}")

        # print the order_no:
        order_no=response['Messages'][0]['MessageAttributes']['order_no']['StringValue']
        logger.info(f"order number received: {order_no}")
        print(f"Order No: {response['Messages'][0]['MessageAttributes']['order_no']['StringValue']}")
        
        #receipt for deleting later
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        logger.info("response handle received.")
        return {'order_no':order_no,'word':word,'receipt handle':receipt_handle}

    except Exception as e:
        print(f"Error getting message: {e}")
        raise e
@task
def delete_message(queue_url, receipt_handle):
    logger=get_run_logger()
    try:
        response = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print(f"Response deleted: {response}")
        logger.info("Message deleted")
    except Exception as e:
        print(f"Error deleting message: {e}")
        raise e
@task
def assemble_message(messages):
    logger=get_run_logger()
    try:



@flow
def sqs_pipeline():
    logger=get_run_logger()
    populate=populate_message(url)
    messages=[]
    messages_checked=0
    if not populate:
        logger.error("Cannot populate messages")
        return False
    while messages_checked!=21:
        messages_available=get_queue_attributes(populate)
        received=receive_message(populate)
        if messages_available!=0:
            logger.info("No messages left")
        time.sleep(30)
        