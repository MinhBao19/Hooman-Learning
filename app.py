#!/bin/python

from flask import Flask, render_template, request, redirect, url_for
from flask_login import LoginManager, login_required, UserMixin
from lib.OpenAIInterface import  Top3Product, ProductSummary
from lib.SQLHandler import db_count_matches, query_top_products
from flask_login import LoginManager, login_required, UserMixin, login_user
from lib.User import User

app = Flask(__name__)

# Setup login 
app.config['SECRET_KEY'] = 'syncs2025' 

login_manager = LoginManager()
login_manager.init_app(app)

# dummy username and password
USERNAME = "syncs"
PASSWORD = "2025"

# Users Dictionary 
users = {}

saved_text = ""

# Not actual password protection
@login_manager.user_loader
def user_loader(user_id):
    return "user"

# Login page
@app.route("/", methods=["GET", "POST"])
def login():

    if request.method == 'POST':
        # Handle login logic here (e.g., validate credentials)
        username = request.form.get('username')
        password = request.form.get('password')
        
        if username == USERNAME and password == PASSWORD:  
            
            # Login user and redirect page - no user attributes
            user = User()
            user.id = username
            users[user] = user.id
            
            login_user(user)
            return redirect(url_for('index'))  # Redirect to a protected page
        
        else:

            message = 'Invalid credentials. Please try again.'
            return render_template('login.html', message=message)
    
    # Render the login page for GET requests
    return render_template('login.html')
        



# Home page   
@app.route('/home')
# @login_required
def index():
    return render_template("index.html", saved_text=saved_text)

# User page
@app.route('/user')
# @login_required
def user():
    return render_template("user.html")



@app.route("/submit", methods=["POST"])
def submit_text():

    global saved_text

    # get the value from the form (the "name" of the input box)
    user_item = request.form.get("user_item")
    user_category = request.form.get("user_category")
    user_supplier = request.form.get("user_supplier")
    user_args = request.form.get("user_args")

    saved_text = f"supplier: {user_supplier}, item_name:{user_item}, sort_by:{user_category}"
    

    #if statement: query db and look up summary : default
    match_count = db_count_matches(user_item, user_supplier)
    print(match_count)
    if match_count > 3:
        print("DB mode")
        # get top 3 rows
        rows = query_top_products(user_item, user_supplier, topn=3)
        # make summaries with API call
        lines = []
        for i, r in enumerate(rows, 1):
            summary = ProductSummary(r)  #API call from helper method
            price = r.get("price_per_unit_aud")
            price_str = f"${price:.2f}" if isinstance(price, (int, float)) else "—"
            lines.append(
                f"{i}. {r.get('description') or '(no name)'} | {r.get('brand_owner') or ''} | "
                f"{price_str} per unit | H{r.get('rating_healthiness') or '–'}/10 "
                f"S{r.get('rating_sustainability') or '–'}/10 | {summary}"
            )
        output_text = "\n".join(lines) if lines else "No local matches."
    else:
        print("Normal mode")
        output_text = Top3Product(saved_text)

    return render_template("index.html", saved_text=saved_text, output_text=output_text)



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80, debug=True)
