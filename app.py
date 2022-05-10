from flask import Flask, render_template, request, jsonify
from utils.kafkaUtils import *

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/<bootstrap_servers>')
def get_list(bootstrap_servers):
    print(request.path)
    print(bootstrap_servers)
    print(bootstrap_servers.split("="))
    bootstrap_servers = bootstrap_servers.split("=")[1]
    print(bootstrap_servers)
    KafkaUtils = kafkaUtils(bootstrap_servers)
    str_topics = KafkaUtils.get_kafka_topics()
    return jsonify(str_topics)

@app.route('/<name>/<topic_name>')
def get_messages(name, topic_name):
    print(request.path)
    bootstrap_servers = name.split("=")[1]
    print(bootstrap_servers)
    print(topic_name)
    KafkaUtils = kafkaUtils(bootstrap_servers)
    list_messages = KafkaUtils.get_messages_from_topic(topic_name)
    list_messages = ["</br>" + x.decode("utf-8") for x in list_messages]
    return str(list_messages)

if __name__ == '__main__':
    app.run(debug=True)