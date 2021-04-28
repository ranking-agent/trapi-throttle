FROM python:3.9

# Add image info
LABEL org.opencontainers.image.source https://github.com/ranking-agent/trapi-throttle

# set up requirements
WORKDIR /app

# Normal requirements
ADD requirements-lock.txt .
RUN pip install -r requirements-lock.txt

# Copy in files
ADD . .

# Set up server
ENTRYPOINT ["python"]
CMD [ "run.py" ]