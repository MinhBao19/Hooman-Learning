#!/bin/python

from flask import Flask, render_template, request

app = Flask(__name__)

saved_text = ""

@app.route('/')
def index():
    return render_template("index.html", saved_text=saved_text)

@app.route("/submit", methods=["POST"])
def submit_text():
    
    global saved_text
    # get the value from the form (the "name" of the input box)
    user_item = request.form.get("user_item")
    user_category = request.form.get("user_category")
    user_args = request.form.get("user_args")

    saved_text = f"{user_item}, {user_category}, {user_args}"
    
    output_text = f"User entered: {saved_text}"  # display to output_text 

    return render_template("index.html", saved_text=saved_text, output_text=output_text)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80, debug=True)
