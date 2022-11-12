FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY ufcstats_t1.py ${LAMBDA_TASK_ROOT}  
COPY t1_helper.py ${LAMBDA_TASK_ROOT}
COPY t1_exceptions.py ${LAMBDA_TASK_ROOT}
COPY configfile.py ${LAMBDA_TASK_ROOT}

CMD ["first_transformer.main"]