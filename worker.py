import boto3
import cv2
import json
import os
import numpy as np
from botocore.exceptions import NoCredentialsError, ClientError
from mpi4py import MPI
import threading
import logging
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    s3_client = boto3.client('s3', region_name='eu-north-1')
    sqs_client = boto3.client('sqs', region_name='eu-north-1')
    bucket_name = 'mohaned-distributing-bucket'
    queue_url = 'https://sqs.eu-north-1.amazonaws.com/339712946444/image-processing-queue'
else:
    s3_client = None
    sqs_client = None
    bucket_name = None
    queue_url = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WorkerThread:
    def __init__(self):
        self.lock = threading.Lock()

    def receive_task(self):
        while True:
            try:
                with self.lock:
                    response = sqs_client.receive_message(
                        QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=10)
                if 'Messages' in response:
                    message = response['Messages'][0]
                    receipt_handle = message['ReceiptHandle']
                    task = json.loads(message['Body'])
                    s3_location = task.get('s3_location')
                    operation = task.get('operation')
                    if s3_location and operation:
                        with self.lock:
                            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                        logging.info(f"Received task with s3_location: {s3_location}, operation: {operation}")
                        return s3_location, operation
                else:
                    return None
            except (ClientError, NoCredentialsError, json.JSONDecodeError) as e:
                logging.error(f"Error receiving task: {e}")

    def process_image(self, img, operation):
        if operation == 'edgedetection':
            result = cv2.Canny(img, 100, 200)
        elif operation == 'colorinversion':
            result = cv2.bitwise_not(img)
        elif operation == 'blur':
            result = cv2.GaussianBlur(img, (9, 9), 0)
        elif operation == 'erosion':
            se_rect = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
            result = cv2.erode(img, se_rect, iterations=1)
        elif operation == 'dilate':
            se_rect = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
            result = cv2.dilate(img, se_rect, iterations=1)
        else:
            result = img
        return result

    def send_result(self, result):
        message_body = f"s3://{bucket_name}/{result}"
        logging.info(f"Sending result message: {message_body}")
        retry_attempts = 3
        for attempt in range(retry_attempts):
            try:
                with self.lock:
                    sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)
                break
            except (ClientError, NoCredentialsError) as e:
                logging.error(f"Error sending result message (attempt {attempt+1}/{retry_attempts}): {e}")
                if attempt < retry_attempts - 1:
                    time.sleep(2 ** attempt)
                else:
                    logging.error(f"Failed to send result message after {retry_attempts} attempts")

    def run(self):
        while True:
            if rank == 0:
                task = self.receive_task()
                if task is None:
                    break
                image, operation = task
                file_name = os.path.basename(image)
                try:
                    s3_client.download_file(bucket_name, file_name, file_name)
                    logging.info(f"Downloaded file: {file_name}")
                except (ClientError, NoCredentialsError) as e:
                    logging.error(f"Error downloading file: {e}")
                    continue

                img = cv2.imread(file_name, cv2.IMREAD_COLOR)
                img_parts = np.array_split(img, size, axis=0)
            else:
                img_parts = None
                operation = None

            operation = comm.bcast(operation, root=0)
            if operation is None:
                break

            img_part = comm.scatter(img_parts, root=0)
            processed_part = self.process_image(img_part, operation)
            processed_parts = comm.gather(processed_part, root=0)

            if rank == 0:
                final_image = np.vstack(processed_parts)
                result_file_name = "result_" + file_name
                cv2.imwrite(result_file_name, final_image)
                try:
                    s3_client.upload_file(result_file_name, bucket_name, result_file_name)
                    self.send_result(result_file_name)
                except (ClientError, NoCredentialsError) as e:
                    logging.error(f"Error uploading result file: {e}")

if __name__ == "__main__":
    logging.info("Starting worker...")
    worker = WorkerThread()
    worker.run()
