from flask import Flask, render_template, request, url_for, redirect, send_from_directory, flash, render_template_string
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin, login_user, LoginManager, login_required, current_user, logout_user
import os
import hashlib
import pymssql
import csv
import jwt
import datetime
import uuid

connectedAppClientId = os.environ['CONNECTED_APP_CLIENT_ID']
connectedAppSecretKey = os.environ['CONNECTED_APP_SECRET_KEY']
connectedAppSecretId = os.environ['CONNECTED_APP_SECRET_ID']
user = os.environ['USER']
server = os.environ['AZURE_SQL_SERVER']
database = os.environ['AZURE_SQL_DATABASE']
username = os.environ['AZURE_SQL_USER']
password = os.environ['AZURE_SQL_PASSWORD']


def get_token():
    return jwt.encode(
        {
            "iss": connectedAppClientId,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=30),
            "jti": str(uuid.uuid4()),
            "aud": "tableau",
            "sub": user,
            "scp": ["tableau:views:embed", "tableau:metrics:embed"],
            "Region": "East"
        },
        connectedAppSecretKey,
        algorithm="HS256",
        headers={
            'kid': connectedAppSecretId,
            'iss': connectedAppClientId
        }
    )


UPLOAD_FOLDER = './static/files/'

app = Flask(__name__)

app.config['SECRET_KEY'] = 'secret-key-goes-here'
app.config['SQLALCHEMY_DATABASE_URI'] = 'mssql+pymssql://' + \
    username+':'+password+'@'+server+':1433/'+database
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

db = SQLAlchemy(app)

login_manager = LoginManager()
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

# CREATE TABLE


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(1000), unique=True)
    username = db.Column(db.String(1000), unique=True)
    password = db.Column(db.String(1000))
    files_uploaded = db.Column(db.String(255))


with app.app_context():
    db.create_all()


@app.route('/')
def home():
    if current_user.is_authenticated:
        return redirect(url_for('choices'))
    return render_template("index.html")


@app.route('/register', methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('choices'))
    if request.method == "POST":
        new_user = User(
            email=request.form.get('email'),
            username=request.form.get('username'),
            password=hashlib.md5(request.form.get(
                'password').encode()).hexdigest(),
            files_uploaded='No'
        )

        db.session.add(new_user)
        db.session.commit()
        # Log in and authenticate user after adding details to database.
        login_user(new_user)

        return redirect(url_for('choices'))

    return render_template("register.html")


@app.route('/login', methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('choices'))
    if request.method == "POST":
        username = request.form.get('username')
        password = hashlib.md5(request.form.get(
            'password').encode()).hexdigest()

        # Find user by email entered.
        user = User.query.filter_by(username=username).first()

        if user:
            # Check stored password hash against entered password hashed.
            if user.password == password:
                login_user(user)
                return redirect(url_for('choices'))
        else:
            return render_template("login.html")

    return render_template("login.html")


@app.route('/uploadfiles', methods=["GET", "POST"])
@login_required
def uploadfiles():
    if request.method == "POST":
        user = User.query.filter_by(username=current_user.username).first()
        user_hash = hashlib.md5(current_user.username.encode()).hexdigest()
        file = request.files['households']
        file.save(os.path.join(
            app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))
        conn = pymssql.connect(
            server=server, database=database, user=username, password=password)
        cursor = conn.cursor()
        with open(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename), 'r') as f:
            reader = csv.reader(f)
            next(reader)  # skip the first row (header)
            for row in reader:
                row = [None if val.strip() == 'null' else val.strip()
                       for val in row]
                cursor.execute("INSERT INTO household_data (HSHD_NUM, L, AGE_RANGE, MARITAL, INCOME_RANGE, HOMEOWNER, HSHD_COMPOSITION, HH_SIZE, CHILDREN) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))

        file = request.files['products']
        file.save(os.path.join(
            app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))

        with open(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename), 'r') as f:
            reader = csv.reader(f)
            next(reader)  # skip the first row (header)
            for row in reader:
                row = [None if val.strip() == 'null' else val.strip()
                       for val in row]
                cursor.execute("INSERT INTO product_data (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES (%s, %s, %s, %s, %s)", (
                    row[0], row[1], row[2], row[3], row[4]))

        file = request.files['transactions']
        file.save(os.path.join(
            app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))

        with open(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename), 'r') as f:
            reader = csv.reader(f)
            next(reader)  # skip the first row (header)
            batch_size = 10000
            batch = []
            op = "INSERT INTO transaction_data (BASKET_NUM,HSHD_NUM,PURCHASE_DATE,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            for row in reader:
                row = [None if val.strip() == 'null' else val.strip()
                       for val in row]
                batch.append(tuple(row))
                if len(batch) >= batch_size:
                    cursor.executemany(op, batch)
                    batch = []
            if len(batch) != 0:
                cursor.executemany(op, batch)

        conn.commit()
        user.files_uploaded = 'Yes'
        db.session.commit()
        login_user(user)
        return redirect(url_for('datapullhousenum'))
    return render_template("uploadfiles.html")


@app.route('/choices', methods=["GET", "POST"])
@login_required
def choices():
    if request.method == "POST":
        pass
    return render_template("choices.html")


@app.route('/datapullhousenum', methods=["GET", "POST"])
@login_required
def datapullhousenum():
    if request.method == "POST":
        pass
    return render_template('/datapull_housenum.html', jwt=get_token())


@app.route('/demographicfactors', methods=["GET", "POST"])
@login_required
def demographicfactors():
    if request.method == "POST":
        pass
    return render_template("demographicfactors.html", jwt=get_token())


@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('home'))


if __name__ == "__main__":
    app.run()
