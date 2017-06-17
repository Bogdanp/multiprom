from flask import Flask, make_response
from multiprom import Collector

app = Flask(__name__)
collector = Collector(namespace="flask")
collector.start()
total_requests = collector.counter(
    "requests_total",
    description="A counter for the total number of requests made to this server."
)


@app.route("/")
def index():
    total_requests.inc()
    return "Hello World!"


@app.route("/metrics")
def metrics():
    data = collector.query()
    response = make_response(data)
    response.headers["content-type"] = "text/plain; version=0.0.4"
    return response
