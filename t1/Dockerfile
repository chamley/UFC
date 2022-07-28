FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY extractor.py ${LAMBDA_TASK_ROOT}  
COPY e1_helper.py ${LAMBDA_TASK_ROOT}

CMD ["first_transformer.main"]





