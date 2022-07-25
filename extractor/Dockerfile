FROM public.ecr.aws/lambda/python:3.8

COPY extractor/extractor.py ${LAMBDA_TASK_ROOT}

COPY extractor/requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["python3", "extractor.py"]





