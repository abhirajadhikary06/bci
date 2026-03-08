from __future__ import annotations

import os

from flask import Flask, render_template

app = Flask(__name__, template_folder="templates", static_folder="static")


@app.get("/")
def index():
	return render_template("index.html")


@app.get("/docs")
def docs():
	return render_template("docs.html")


if __name__ == "__main__":
	app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
