FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY configfile.py ${LAMBDA_TASK_ROOT}
COPY src/loader/dbhelper_loader.py ${LAMBDA_TASK_ROOT}  
COPY src/loader/loaderExceptions.py ${LAMBDA_TASK_ROOT}
COPY src/loader/preloader.py ${LAMBDA_TASK_ROOT}

CMD ["preloader.main"]




