FROM python:3.11 
# Or any preferred Python version.
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "./main.py"]
# Or enter the name of your unique directory and parameter set.