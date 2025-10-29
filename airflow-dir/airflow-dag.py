from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.sdk import Variable

from datetime import datetime
import requests
import boto3
import time
import pandas as pd
import logging
from airflow.models import XCom

url='https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/jac7az'
uvaid='jac7az'
submit_url='https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit'
default_args={'owner':'airflow',
              'depends_on_past':False,
              'email_on_failure':False,
              'email_on_retry':False,
              'retries':1}

def sqs_client():
    return boto3.client('sqs',region_name='us-east-1',aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
                   aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'))  

#Task1: Populate the message queue with all 21 messages
def populate_message(url):
    logger = logging.getLogger("airflow.task")
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
    #Task3: Assembling the message together by putting it through a dataframe, sorting it and concatenating the words into one message with assemble_message(). Then, the final solution is sent back to the given aws URL with send_url().\=    
def assemble_message(url):
    message_list=[]
    messages_checked=0
    logger = logging.getLogger("airflow.task")
    sqs=sqs_client()
    try:    #get_queue_attributes() from prefect
        while messages_checked!=21:
            try:
                response = sqs.get_queue_attributes(QueueUrl=url,AttributeNames=['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible','ApproximateNumberOfMessagesDelayed'])                                      
                attributes=response['Attributes']
                num_messages=int(attributes.get('ApproximateNumberOfMessages',0))
                num_invis=int(attributes.get('ApproximateNumberOfMessagesNotVisible',0))
                num_delay=int(attributes.get('ApproximateNumberOfMessagesDelayed',0))
                logger.info(f"{num_messages} available. {num_invis+num_delay} left.")
                print(f"Response: {response}")          
            except Exception as e:                                                                                 
                logger.error(f"Error getting queue attributes: {e}")                                                      
                raise e
            
            try:    #receive_message() from prefect
                response = sqs.receive_message(
                QueueUrl=url,
                MessageSystemAttributeNames=['All'],
                VisibilityTimeout=60,
                MessageAttributeNames=['order_no','word'],
                WaitTimeSeconds=10
            )
                messages=response.get('Messages','')
                if not messages:
                    logger.info("Messages empty. Pausing for 15sec")
                    time.sleep(15)
                else:
                    logger.info("Message received. Extracting receipt handle, word and order number.")
                    word=response['Messages'][0]['MessageAttributes']['word']['StringValue']
                    logger.info(f"word received: {word}")

                    order_no=response['Messages'][0]['MessageAttributes']['order_no']['StringValue']
                    logger.info(f"order number received: {order_no}")
                    
                    #receipt for deleting later
                    receipt_handle = response['Messages'][0]['ReceiptHandle']
                    logger.info(f"response handle received: {receipt_handle}")
                    message_list.append({'order_no':order_no,'word':word})
                    messages_checked+=1
                    logger.info(f"Current messages checked: {messages_checked}")

                    #delete_message() from prefect
                    try:
                        sqs.delete_message(QueueUrl=url,ReceiptHandle=receipt_handle)
                        logger.info("Message deleted")
                    except Exception as e:
                        logger.error("Could not delete message")
            except Exception as e:
                logger.error(f"Error getting message: {e}")
        df=pd.DataFrame(message_list)
        df['order_no']=df['order_no'].astype(int)
        sort_df=df.sort_values(by='order_no').reset_index(drop=True)
        phrase=" ".join(sort_df['word'])
        logger.info(f"Final phrase assembled: {phrase}")
        return phrase
    except Exception as e:
        logger.error(f'Assembly error: {e}')
        raise e
    
def send_solution(url,uvaid, phrase, platform): #sends the solution to the given URL.
    sqs=sqs_client()
    logger = logging.getLogger("airflow.task")
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
        
#Assembling the DAG
with DAG('sqs_pipeline',
        default_args=default_args,
        start_date=datetime(2025,10,27),
        schedule=None,
        catchup=False
) as dag:
    populate=PythonOperator(
        task_id='populate_message',
        python_callable=populate_message,
        op_kwargs={'url':url},
    )
    phrase=PythonOperator(
        task_id='process_and_assemble_messages',
        python_callable=assemble_message,
        op_kwargs={'url':"{{task_instance.xcom_pull(task_ids='populate_message')}}"},
    )
    submission=PythonOperator(
        task_id='send_solution',
        python_callable=send_solution,
        op_kwargs={'url':submit_url,
                   'uvaid':uvaid,
                   'phrase':"{{task_instance.xcom_pull(task_ids='assemble_message')}}",
                   'platform':'airflow'},
    )
chain(populate,phrase,submission)