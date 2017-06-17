# multiprom gunicorn example

1. `pip install flask gunicorn`
1. `env PYTHONPATH="../.." gunicorn --workers 8 example:app`
1. `open http://localhost:8000`
1. finally `open http://localhost:8000/metrics`
