#!/bin/python

from flask import Flask, render_template, request, redirect, url_for
from flask_login import LoginManager, login_required, UserMixin
from lib.OpenAIInterface import callOpenAI

app = Flask(__name__)

# Setup login 
app.config['SECRET_KEY'] = 'syncs2025' 

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # Set the login view to the 'login' route

saved_text = ""

@app.route('/')
@login_required
def index():
    return render_template("index.html", saved_text=saved_text)


@app.route('/user')
@login_required
def user():
    return render_template("user.html")


@app.route('/login', methods=["GET", "POST"])
def login():
     if request.method == 'POST':
        # Handle login logic here (e.g., validate credentials)
        username = request.form['username']
        password = request.form['password']
        
        if username == 'user' and password == 'pass':  
            return redirect(url_for('dashboard')) # Redirect to a protected page
        else:
            message = 'Invalid credentials. Please try again.'
            return render_template('login.html', message=message)



@app.route("/submit", methods=["POST"])
def submit_text():
    
    global saved_text
    # get the value from the form (the "name" of the input box)
    user_item = request.form.get("user_item")
    user_category = request.form.get("user_category")
    user_args = request.form.get("user_args")

    saved_text = f"supplier: Woolworths, item_name:{user_item}, sort_by:{user_category}"
    
    output_text = callOpenAI(saved_text)


    return render_template("index.html", saved_text=saved_text, output_text=output_text)



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80, debug=True)
