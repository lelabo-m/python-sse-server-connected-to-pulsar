#!/usr/bin/env python
import flask
import pulsar


app = flask.Flask(__name__)
client = pulsar.Client('pulsar://localhost:6650')

def event_stream():
    consumer = client.subscribe('my-topic', 'my-subscription')
    while True:
        msg = consumer.receive()
        consumer.acknowledge(msg)
        yield ("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))


@app.route('/stream')
def stream():
    return flask.Response(event_stream(), mimetype="text/event-stream")


@app.route('/')
def home():
    return "Hello World"


if __name__ == '__main__':
    app.debug = True
    app.run()
