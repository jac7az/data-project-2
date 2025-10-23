from prefect import flow, task, get_run_logger
import requests
import boto3

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
            return num_messages+num_invis+num_delay
        else:
            logger.error("Error in getting queue messages")
                                                                 
    except Exception as e:                                                                                 
        print(f"Error getting queue attributes: {e}")                                                      
        raise e  
    
@task
def receive_message(url):
    # try to get any messages with message-attributes from SQS queue:
    try:
        response = sqs.receive_message(
            QueueUrl=url,
            MessageSystemAttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=60,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=10
        )
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        delete_message(url, receipt_handle)

        # print the word:
        word=response['Messages'][0]['MessageAttributes']['word']['StringValue']
        print(f"MessageAttributes: {response['Messages'][0]['MessageAttributes']}")

        # print the order_no:
        order_no=response['Messages'][0]['MessageAttributes']['order_no']['StringValue']
        print(f"Order No: {response['Messages'][0]['MessageAttributes']['order_no']['StringValue']}")
        return response['Messages'][0]

    except Exception as e:
        print(f"Error getting message: {e}")
        raise e
@task
def delete_message(queue_url, receipt_handle):
    try:
        response = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print(f"Response: {response}")
    except Exception as e:
        print(f"Error deleting message: {e}")
        raise e