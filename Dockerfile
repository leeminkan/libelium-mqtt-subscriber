FROM python:3.7.5-slim
WORKDIR /usr/src/app
COPY . .
# install dependencies
RUN python -m pip install --upgrade pip -i https://pypi.douban.com/simple
RUN pip install -r requirements.txt

CMD ["python", "subscriber.py"]