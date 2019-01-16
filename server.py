#!/usr/bin/env python
import flask
import pulsar


app = flask.Flask(__name__)
client = pulsar.Client('pulsar://localhost:6650')

def event_stream():
    print("START STREAM")
    consumer = client.subscribe('my-topic', 'my-subscription')
    while True:
        msg = consumer.receive()
        consumer.acknowledge(msg)
        yield ("data: {}\n".format(msg.data(), msg.message_id()))


@app.route('/stream')
def stream():
    print("ROUTE STREAM")
    return flask.Response(event_stream(), mimetype="text/event-stream")


@app.route('/')
def home():
    print("ROUTE HOME")
    return "Hello World"


if __name__ == '__main__':
    app.run(host='0.0.0.0')
