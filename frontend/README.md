# Joey Frontend Interface

The Joey frontend interface lets you visually create patterns and analyze Parquet files. 

First pull the Docker image:

docker pull marsupialtail/joey_demo:latest

Now run the Docker image and mount the directory containing your Parquet files to app/data.

docker run -p 8866:8866 -v /home/my_data:/app/data joey_demo:latest

Now go to localhost:8866.
