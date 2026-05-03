FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY stream_demo.py .

CMD ["stream_demo.lambda_handler"]