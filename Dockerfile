FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py /usr/src/app

# Run the Python script when the container launches
# Use the environment variables to pass the arguments
CMD python main.py