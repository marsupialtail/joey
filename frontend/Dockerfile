# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Install Voilà
RUN pip3 install --no-cache-dir ipywidgets jupyterlab notebook voila pandas polars duckdb matplotlib
RUN pip3 install --no-cache-dir pyjoey

# Set the working directory in the container
WORKDIR /app
RUN mkdir /app/data

# Copy the current directory contents into the container at /usr/src/app
COPY example.ipynb /app

# Run voila on container start, serving the notebook
CMD ["voila", "--no-browser", "--Voila.ip=0.0.0.0", "example.ipynb"]

