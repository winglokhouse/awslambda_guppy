# Source from docker hub
FROM amazon/aws-lambda-python:3.7

# Install various packages to get compile environment
RUN yum update -y && \
	# Fix unable to execute 'gcc': No such file or directory
	yum groupinstall -y "Development Tools" && \
	# Fix Talib/_ta_lib.c:4:10: fatal error: Python.h: no such file or directory #include "Python.h"
	yum install -y python36-devel.x86_64 && \
	yum install -y wget && \
	yum install -y sudo && \
	#
	wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
	tar xvf ta-lib-0.4.0-src.tar.gz && \
	cd ta-lib && \
	./configure --prefix=/usr && \
	make && \
	sudo make install && \
	#
	# Fix error message: ImportError: libta_lib.so.0: cannot open shared object file:
	sudo echo “include /usr/lib” >> /etc/ld.so.conf && \
	sudo ldconfig

# shell command : WARNING: You are using pip version 20.1.1; however, version 21.0.1 is available
RUN /var/lang/bin/python3.7 -m pip install --upgrade pip
# copy from this linux machine to container
COPY requirements-lambda-yfissue.txt .
RUN pip install -r requirements-lambda-yfissue.txt

COPY src/ .	
CMD ["lambda_create_ohlc_csv.lambda_handler"]