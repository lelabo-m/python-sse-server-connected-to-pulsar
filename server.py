#!/usr/bin/env python
import flask
import pulsar
import re

app = flask.Flask(__name__)
client = pulsar.Client('pulsar://localhost:6650')


def event_stream():
    print("START STREAM")
    consumer = client.subscribe(re.compile('persistent://public/default/topics-.*'), 'my-subscription',
                                pattern_auto_discovery_period=0)
    while True:
        msg = consumer.receive()
        print(f"RECEIVED: {msg}")
        consumer.acknowledge(msg)
        # yield ("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        out = 'event: message\n'
        out += 'data: %s\n' % msg.data()
        out += '\n'
        yield(out)


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
