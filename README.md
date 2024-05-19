# distributed-image-processing-using-cloud
Image Processing System
Introduction
This project is an image processing system using AWS S3 and SQS for scalable and fault-tolerant image processing. It includes a GUI for uploading images and selecting processing operations.

Project Structure
.
├── master.py
├── worker.py
├── README.md
Installation
Prerequisites
Python 3.8+
Docker
AWS CLI configured
Steps
Clone the Repository

bash
Copy code
git clone [https://github.com/yourusername/image-processing-system.git](https://github.com/mohaned-nasr/distributed-image-processing-using-cloud.git)
cd image-processing-system
Install Dependencies

bash
Copy code
pip install -r requirements.txt
Run Docker Containers

bash
Copy code
docker build -t image-processor .
docker run -d --name image-processor image-processor
Usage
Run the GUI

bash
Copy code
python master.py
Upload Images

Click "Upload Image" or "Upload Multiple Images".
Select the processing operation.
Processed images will be displayed in the GUI.

System Architecture
master.py: GUI for uploading images and selecting operations.
worker.py: Processes images from SQS tasks, uploads results to S3.
License
This project is licensed under the MIT License.
