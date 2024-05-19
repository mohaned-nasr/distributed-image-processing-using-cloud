import os
import logging
from tkinter import filedialog, ttk
import tkinter as tk
from botocore.client import ClientError
import boto3
import cv2
import json
import threading
from PIL import Image, ImageTk
import time
import sys


s3_client = boto3.client('s3', region_name='eu-north-1')
sqs_client = boto3.client('sqs', region_name='eu-north-1')

bucket_name = 'mohaned-distributing-bucket'
queue_url = 'https://sqs.eu-north-1.amazonaws.com/339712946444/image-processing-queue'

lock = threading.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OutputRedirector(object):
    def __init__(self, text_widget):
        self.text_widget = text_widget

class RedirectedStdout(OutputRedirector):
    def write(self, message):
        self.text_widget.insert(tk.END, message)
        self.text_widget.see(tk.END)

def s3_upload(file_path, bucket, object_name=None):
    if object_name is None:
        object_name = file_path
    retry_attempts = 3
    for attempt in range(retry_attempts):
        try:
            s3_client.upload_file(file_path, bucket, object_name)
            logging.info(f"Uploaded {file_path} to {bucket}/{object_name}")
            return True
        except ClientError as e:
            logging.error(f"Error uploading file (attempt {attempt+1}/{retry_attempts}): {e}")
            if attempt < retry_attempts - 1:
                time.sleep(2 ** attempt)
    return False

def sqs_send_message(queue_url, message_body):
    retry_attempts = 3
    for attempt in range(retry_attempts):
        try:
            with lock:
                sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)
            logging.info(f"Sent message: {message_body}")
            time.sleep(2)
            return True
        except ClientError as e:
            logging.error(f"Error sending message (attempt {attempt+1}/{retry_attempts}): {e}")
            if attempt < retry_attempts - 1:
                time.sleep(2 ** attempt)
    return False

def s3_download(bucket, object_name, file_path):
    retry_attempts = 3
    for attempt in range(retry_attempts):
        try:
            s3_client.download_file(bucket, object_name, file_path)
            logging.info(f"Downloaded {bucket}/{object_name} to {file_path}")
            return True
        except ClientError as e:
            logging.error(f"Error downloading file (attempt {attempt+1}/{retry_attempts}): {e}")
            if attempt < retry_attempts - 1:
                time.sleep(2 ** attempt)
    return False

def process_sqs_messages(progress_bar, image_display_label):
    logging.info("Processing messages...")
    progress_bar['value'] += 1
    while True:
        with lock:
            response = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        messages = response.get('Messages', [])
        if messages:
            logging.info(f"Received {len(messages)} message(s)")
            for message in messages:
                logging.info(f"Received message: {message['Body']}")
                progress_bar['value'] += 1
                if message['Body'].startswith("s3://"):
                    s3_location = message['Body']
                    bucket, object_name = s3_location.replace("s3://", "").split("/", 1)
                    new_file_path = os.path.join(os.getcwd(), object_name)
                    if s3_download(bucket, object_name, new_file_path):
                        img = cv2.imread(new_file_path)
                        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
                        img = Image.fromarray(img)
                        img.thumbnail((400, 400))
                        img = ImageTk.PhotoImage(img)
                        image_display_label.config(image=img)
                        image_display_label.image = img
                        with lock:
                            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
        else:
            break
        time.sleep(5)

def upload_file(file_path, operation, progress_bar):
    file_name = os.path.basename(file_path)
    if s3_upload(file_path, bucket_name, file_name):
        task = {'s3_location': f"s3://{bucket_name}/{file_name}", 'operation': operation}
        if sqs_send_message(queue_url, json.dumps(task)):
            progress_bar['value'] += 10

def upload_files(file_paths, operation, progress_bar):
    for file_path in file_paths:
        upload_file(file_path, operation, progress_bar)

def choose_and_upload_files(operation, progress_bar):
    file_paths = filedialog.askopenfilenames()
    if file_paths:
        threading.Thread(target=upload_files, args=(file_paths, operation, progress_bar)).start()

def main_gui():
    root = tk.Tk()
    root.title("Distributed Image Processing")

    ttk.Style().configure("TButton", padding=6, relief="flat", background="#ccc")
    ttk.Style().configure("red.Horizontal.TProgressbar", troughcolor='white', background='#ff0000')

    operations = ["edgedetection", "colorinversion", "blur", "erosion", "dilate"]

    label = ttk.Label(root, text="Choose an operation:")
    label.pack(pady=20)

    operation_var = tk.StringVar(value=operations[0])
    operation_menu = ttk.Combobox(root, textvariable=operation_var, values=operations, state="readonly")
    operation_menu.pack(pady=20)

    upload_button = ttk.Button(root, text="Upload and Process Image",
                               command=lambda: choose_and_upload_files(operation_var.get(), progress_bar))
    upload_button.pack(pady=20)

    process_button = ttk.Button(root, text="Process SQS Messages",
                                command=lambda: threading.Thread(target=process_sqs_messages, args=(progress_bar, image_display_label)).start())
    process_button.pack(pady=20)

    upload_multiple_button = ttk.Button(root, text="Upload and Process Multiple Images",
                                        command=lambda: choose_and_upload_files(operation_var.get(), progress_bar))
    upload_multiple_button.pack(pady=20)

    progress_bar = ttk.Progressbar(root, style="red.Horizontal.TProgressbar", length=400, mode='determinate')
    progress_bar.pack(pady=20)

    console_frame = tk.Frame(root)
    console_frame.pack(fill=tk.BOTH, expand=True)
    console_text = tk.Text(console_frame, wrap=tk.WORD, state=tk.DISABLED)
    console_text.pack(fill=tk.BOTH, expand=True)
    sys.stdout = RedirectedStdout(console_text)

    image_display_label = tk.Label(root)
    image_display_label.pack(pady=20)

    root.mainloop()

if __name__ == "__main__":
    main_gui()
