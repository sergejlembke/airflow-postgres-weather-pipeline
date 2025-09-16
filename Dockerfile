# Copy requirements and install them
# Note: The base image already contains Airflow. Installing apache-airflow again
# from requirements.txt should match the base version; if you see conflicts,
# consider removing apache-airflow from requirements.txt.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Default CMD/ENTRYPOINT come from the base image