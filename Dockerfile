FROM python:3.9.16-alpine3.16
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
EXPOSE 5000
COPY inputform.py /app/
ENTRYPOINT [ "python" , "inputform.py"]
