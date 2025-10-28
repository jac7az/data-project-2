from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
import requests
import boto3
import time
import pandas as pd
import logging

logger = logging.getLogger("airflow.task")
sqs = boto3.client('sqs')  
url='https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/jac7az'
uvaid='jac7az'
submit_url='https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit'

#Task1: Populate the message queue with all 21 messages
@task
def populate_message(url):
    try:
        payload=requests.post(url).json()
        if payload.get('sqs_url'):
            logger.info(f"Got sqs url: {payload.get('sqs_url')}")
            return payload.get('sqs_url')
        else:
            raise ValueError("Link not found.")
    except Exception as e:
        print(f"Request failure in POST: {e}")
        logger.error("Request failure in POST")
        raise e

#Task2: Get the number of messages that need to be processed with get_queue_attributes() and receive those messages with receive_message(). Finally, delete the message with delete_message()
def get_queue_attributes(url):  #Finds the number of messages available, hidden and delayed.
      
    try:                                                                                     
        response = sqs.get_queue_attributes(QueueUrl=url,AttributeNames=['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible','ApproximateNumberOfMessagesDelayed'])                                      
        attributes=response['Attributes']
        num_messages=int(attributes.get('ApproximateNumberOfMessages',0))
        num_invis=int(attributes.get('ApproximateNumberOfMessagesNotVisible',0))
        num_delay=int(attributes.get('ApproximateNumberOfMessagesDelayed',0))
        if 0<num_messages+num_invis+num_delay<=21:
            logger.info(f"{num_messages} available. {num_invis+num_delay} left.")
            print(f"Response: {response}")    
            return num_messages, num_messages+num_invis+num_delay
        else:
            logger.error("Error in getting queue messages")    
            return 
    except Exception as e:                                                                                 
        logger.error(f"Error getting queue attributes: {e}")                                                      
        raise e

def receive_message(url):      #Receives individual messages and extracts the order number and word from them. Also extracts the receipt handle for each message to delete later.
    
    try:
        response = sqs.receive_message(
            QueueUrl=url,
            MessageSystemAttributeNames=['All'],
            VisibilityTimeout=60,
            MessageAttributeNames=['order_no','word'],
            WaitTimeSeconds=10
        )
        if response.get('Messages','')=='':
            logger.error("Messages empty")
            return ''
        logger.info("Message received. Extracting receipt handle, word and order number.")
        word=response['Messages'][0]['MessageAttributes']['word']['StringValue']
        logger.info(f"word received: {word}")

        order_no=response['Messages'][0]['MessageAttributes']['order_no']['StringValue']
        logger.info(f"order number received: {order_no}")
        
        #receipt for deleting later
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        logger.info(f"response handle received: {receipt_handle}")
        return {'order_no':order_no,'word':word,'receipt_handle':receipt_handle}

    except Exception as e:
        logger.error(f"Error getting message: {e}")
        raise e

def delete_message(url, receipt_handle):    #deleting the message to move on to the next one.

    try:
        deleted_response = sqs.delete_message(
            QueueUrl=url,
            ReceiptHandle=receipt_handle
        )
        print(f"Response deleted: {deleted_response}")
        logger.info("Message deleted")
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
        raise e

#Task 3: Assembling the message together by putting it through a dataframe, sorting it and concatenating the words into one message with assemble_message(). Then, the final solution is sent back to the given aws URL with send_url().
def assemble_message(messages): #converting the list of dictionaries containing order number and words into a dataframe to sort and form a message.
   
    try:
        df=pd.DataFrame(messages)
        df['order_no']=df['order_no'].astype(int)
        sort_df=df.sort_values(by='order_no').reset_index(drop=True)
        return " ".join(sort_df['word'])
    except Exception as e:
        logger.error('Cannot form dataframe')
        raise e
    
def send_solution(url,uvaid, phrase, platform): #sends the solution to the given URL.
    try:
        message=f"Solution from {uvaid} using {platform}"
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody=message,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        logger.info(f"Response: {response}")
    except Exception as e:
        logger.error("Couldn't submit response")
        raise e
    
@dag(
    dag_id='sqs_pipeline',
    start_date=datetime(2025,10,27)
)
def sqs_pipeline():

    #runs the populate_message()
    populate=PythonOperator(
        task_id='populate_message',
        python_callable=populate_message,
        op_kwargs={'url':url},
    )
    logger.info(f"Message populated: {populate}")

    @task
    def all_messages(url):
        message_list=[]
        messages_checked=0
        while messages_checked!=21:
            messages_available,total_messages=get_queue_attributes(url) 
            if messages_available==0:
                logger.info("No messages right now. Pausing for 15sec.")
                time.sleep(15)
            else:
                received=receive_message(populate)
                if received=='':
                    logger.info("Empty message.")
                    continue
                else:
                    message_list.append({'order_no':received['order_no'],'word':received['word']})
                logger.info(f"Receipt handle: {received['receipt_handle']}")
                delete_message(populate, received['receipt_handle'])
                messages_checked+=1
                logger.info(f"{messages_checked} messages checked.")
        return message_list
        
    message_list=all_messages(populate.output)
    phrase=PythonOperator(task_id='assemble_messages',
                          python_callable=assemble_message,
                          op_kwargs={'messages':message_list.output},)
    submission=PythonOperator(task_id='send_messages',
                              python_callable=send_solution,
                              op_kwargs={'url':submit_url,
                                         'uvaid':uvaid,
                                         'platform':'airflow',
                                         'phrase':phrase.output,}
                              )
    chain(populate,message_list,phrase,submission)
    logger.info("Submission sent.")
if __name__ == "__main__":
    phrase=sqs_pipeline()
    print(phrase)