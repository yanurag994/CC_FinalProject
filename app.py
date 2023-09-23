from flask import Flask, render_template, request, url_for, redirect #Hosting the webapp
from flask_login import UserMixin, login_user, LoginManager, login_required, current_user, logout_user #Session Management
import os, csv #OS interaction for file storage
import pymssql, hashlib #Connection to Azure and Password Hashin
import jwt, uuid, datetime #JWT authentication with Tableau

app = Flask(__name__)

connectedAppClientId = os.environ['CONNECTED_APP_CLIENT_ID']
connectedAppSecretKey = os.environ['CONNECTED_APP_SECRET_KEY']
connectedAppSecretId = os.environ['CONNECTED_APP_SECRET_ID']
user = os.environ['TABLEAU_USER']
server = os.environ['AZURE_SQL_SERVER']
database = os.environ['AZURE_SQL_DATABASE']
username = os.environ['AZURE_SQL_USER']
password = os.environ['AZURE_SQL_PASSWORD']
port = os.environ['AZURE_SQL_PORT']

app.config['SECRET_KEY'] = os.environ['APP_COOKIE_SECRET']

UPLOAD_FOLDER = './static/files/'

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

conn = pymssql.connect(server=server, database=database, user=username,port=port,password=password)
cursor = conn.cursor()

login_manager = LoginManager()
login_manager.init_app(app)

@login_manager.user_loader
def load_user(user_id):
    # Use pymssql to fetch the user by user_id from the database
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM app_users WHERE id = %s", (user_id,))
        row = cursor.fetchone()
        if row:
            return User(id=row[0],email=row[1], username=row[2], password=row[3], files_uploaded=row[4])
        return None

# CREATE TABLE
class User(UserMixin):
    def __init__(self, id, email, username, password, files_uploaded):
        self.id = id
        self.email = email
        self.username = username
        self.password = password
        self.files_uploaded = files_uploaded

@app.route('/register', methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('choices'))
    if request.method == "POST":
        email = request.form.get('email')
        username = request.form.get('username')
        password = hashlib.md5(request.form.get('password').encode()).hexdigest()

        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO app_users (email, username, password, files_uploaded) VALUES (%s, %s, %s, %s)", (email, username, password, 'No'))
            conn.commit()
            cursor.execute("SELECT * FROM app_users WHERE username = %s", (username,))
            row = cursor.fetchone()
            if row and row[3] == password:
                # Password match, log in the user
                user = User(id=row[0],email=row[1], username=row[2], password=row[3], files_uploaded=row[4])
                login_user(user)
                return redirect(url_for('choices'))

    return render_template("register.html")

@app.route('/login', methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('choices'))
    if request.method == "POST":
        username = request.form.get('username')
        password = hashlib.md5(request.form.get('password').encode()).hexdigest()
        
        # Fetch the user from the database based on the provided username
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM app_users WHERE username = %s", (username,))
            row = cursor.fetchone()
            if row and row[3] == password:
                # Password match, log in the user
                user = User(id=row[0],email=row[1], username=row[2], password=row[3], files_uploaded=row[4])
                login_user(user)
                return redirect(url_for('choices'))
    
    return render_template("login.html")


def process_file(file, op, cursor, batch_size):
    progress_tracker=0
    with file as f:
        reader = csv.reader(f)
        next(reader)  # skip the first row (header)
        batch = []
        for row in reader:
            row = [None if val.strip() == 'null' else val.strip() for val in row]
            batch.append(tuple(row))
            if len(batch) >= batch_size:
                cursor.executemany(op, batch)
                progress_tracker+=1
                batch = []
        if len(batch) != 0:
            cursor.executemany(op, batch)


@app.route('/uploadfiles', methods=["GET", "POST"])
@login_required
def uploadfiles():
    if request.method == "POST":
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM app_users WHERE username = %s", (current_user.username))
            row = cursor.fetchone()

            if row:
                user = User(id=row[0],email=row[1], username=row[2], password=row[3], files_uploaded=row[4])
                user_hash = hashlib.md5(current_user.username.encode()).hexdigest()

                # Process 'households' file
                file = request.files['households']
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))
                op = "INSERT INTO household_data (HSHD_NUM, L, AGE_RANGE, MARITAL, INCOME_RANGE, HOMEOWNER, HSHD_COMPOSITION, HH_SIZE, CHILDREN) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                process_file(open(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename), 'r'), op, cursor, 1000)

                # Process 'products' file
                file = request.files['products']
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))
                op = "INSERT INTO product_data (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES (%s, %s, %s, %s, %s)"
                process_file(open(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename), 'r'), op, cursor, 10000)

                # Process 'transactions' file
                file = request.files['transactions']
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename))
                op = "INSERT INTO transaction_data (BASKET_NUM,HSHD_NUM,PURCHASE_DATE,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                process_file(open(os.path.join(app.config['UPLOAD_FOLDER'], user_hash + '_in_' + file.filename), 'r'), op, cursor, 100000)

                user.files_uploaded = 'Yes'
                login_user(user)
                return redirect(url_for('datapullhousenum'))
            else:
                user = None
                user_hash = None
    return render_template("uploadfiles.html")


@app.route('/choices', methods=["GET", "POST"])
@login_required
def choices():
    if request.method == "POST":
        pass
    return render_template("choices.html")

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

@app.route('/')
def home():
    if current_user.is_authenticated:
        return redirect(url_for('choices'))
    return render_template("index.html")

if __name__ == "__main__":
    app.run()
