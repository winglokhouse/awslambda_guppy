# Source from docker hub
FROM amazon/aws-lambda-python:3.7

# Install various packages to get compile environment
RUN yum update -y
# shell command : WARNING: You are using pip version 20.1.1; however, version 21.0.1 is available
RUN /var/lang/bin/python3.7 -m pip install --upgrade pip
# copy from this linux machine to container
COPY requirements-trend-detect.txt .
RUN pip install -r requirements-trend-detect.txt

COPY src/ .	
CMD ["lambda_trend_detect_rewrite.lambda_handler"]