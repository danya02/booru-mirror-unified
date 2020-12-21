from database import *
from flask import Flask, make_response, abort

app = Flask(__name__)


@app.route('/<file>')
def serve_file(file):
    try:
        file = File.get_file_by_sha256(file, return_instance=True)
    except File.DoesNotExist:
        return abort(404)
    r = make_response(file.content)
    r.mimetype = file.mimetype
    return r

if __name__ == '__main__':
    app.run('0.0.0.0', 5000)
