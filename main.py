from flask import Flask, flash, redirect, url_for, render_template, request
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

from decimal import Decimal

import json
import boto3
import time
import os
import requests

app = Flask(__name__)
app.config['SECRET_KEY'] = "abc123!@#zxc"
db_client = boto3.client("dynamodb")
s3_client = boto3.client("s3")

@app.route('/')
def root():        
    return redirect(url_for("loginpage"))

@app.route('/loginpage')
def loginpage():
    return render_template("index.html")

@app.route('/login', methods = ["POST"])
def login():
    email = request.form.get("email")
    password = request.form.get("password")

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("user")

    response = table.scan(
        FilterExpression = Attr("password").eq(password) & Attr("email").eq(email)
    )

    items = response["Items"]

    # Login successful
    if(len(items) == 1):
        # admin login
        if(items[0]["email"] == "admin"):
            return redirect(url_for("adminpage"))

        # user login
        else:
            return redirect(url_for("mainpage", email = email))

    # Login unsuccessful
    else:
        flash("Email or password is invalid. Please retry.")
        return redirect(url_for("loginpage"))

@app.route('/registerpage')
def registerpage():
    return render_template("registerpage.html")

@app.route('/register', methods = ["POST"])
def register():
    email = request.form.get("email")
    username = request.form.get("username")
    password = request.form.get("password")

    ## Check is email exists
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("user")

    response = table.scan(
        FilterExpression = Attr("email").eq(email)
    )

    items = response["Items"]

    # If email exists
    if(len(items) != 0):
        flash("Email already exists. Please retry.")
        return redirect(url_for("registerpage"))

    # If email does not exist
    else:
        table = dynamodb.Table("user")

        table.put_item(
            Item = {
                "email": email,
                "user_name": username,
                "password": password
            }
        )

        return redirect(url_for("loginpage"))
    
# mainpage.html
@app.route('/mainpage/<string:email>')
def mainpage(email):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("user")

    response = table.scan(
        FilterExpression = Attr("email").eq(email)
    )

    items = response["Items"]

    username = items[0]["user_name"]

    return render_template("mainpage.html", username = username, email = email)

# adminpage.html
@app.route('/adminpage')
def adminpage():
    return render_template("adminpage.html")

# Create dynamodb music table
@app.route('/createtable')
def createtable():
    dynamodb = boto3.resource('dynamodb', region_name = "us-east-1")
    
    error = ""

    try :
        response = db_client.describe_table(TableName = "music")
    except ClientError as ce:
        error = ce.response["Error"]["Code"]

    # Table exists
    if(error != "ResourceNotFoundException"):
        flash("Table has already existed")
        return redirect(url_for("adminpage"))

    # Table does not exist
    else:
        dynamodb = boto3.resource('dynamodb', region_name = "us-east-1")

        table = dynamodb.create_table(TableName = "music",
            KeySchema = [
                {
                    # Parition key
                    "AttributeName": "music_id",
                    "KeyType": "HASH"
                },
                {
                    # Sort key
                    "AttributeName": "title",
                    "KeyType": "RANGE"
                }
            ],
            AttributeDefinitions = [
                {
                    "AttributeName": "music_id",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "title",
                    "AttributeType": "S"
                }
            ],
            ProvisionedThroughput = {
                "ReadCapacityUnits": 10,
                "WriteCapacityUnits": 10
            }
        )

        table.wait_until_exists()

        flash("Table created")
        return redirect(url_for("adminpage"))

# Populate music table
@app.route('/populatetable')
def populatetable():
    dynamodb = boto3.resource('dynamodb', region_name = "us-east-1")

    error = ""

    try :
        response = db_client.describe_table(TableName = "music")
    except ClientError as ce:
        error = ce.response["Error"]["Code"]

    # Table exists
    if(error != "ResourceNotFoundException"):
        # Open the json file in database
        with open("database/a2.json") as json_file:
            music_list = json.load(json_file, parse_float = Decimal)

        table = dynamodb.Table("music")

        musicCounter = 0

        for music in music_list["songs"]:
            # Increment musicCounter then add the value into music_id in table
            musicCounter = musicCounter + 1

            music_id = str(musicCounter)
            title = music["title"]
            artist = music["artist"]
            year = music["year"]
            web_url = music["web_url"]
            img_url = music["img_url"]

            table.put_item(
                Item = {
                    "music_id": music_id,
                    "title": title,
                    "artist": artist,
                    "year": year,
                    "web_url": web_url,
                    "img_url": img_url
                }
            )

        flash("Table populated")
        return redirect(url_for("adminpage"))

    # Table does not exist
    else:
        flash("Table does not exist")
        return redirect(url_for("adminpage"))

# Delete music table
@app.route('/deletetable')
def deletetable():
    dynamodb = boto3.resource('dynamodb', region_name = "us-east-1")

    error = ""

    try :
        response = db_client.describe_table(TableName = "music")
    except ClientError as ce:
        error = ce.response["Error"]["Code"]

    # Table exists, delete it
    if(error != "ResourceNotFoundException"):
        dynamodb = boto3.resource('dynamodb', region_name = "us-east-1")
        table = dynamodb.Table("music")
        table.delete()

        flash("Table is deleted")
        return redirect(url_for("adminpage"))

    # Table does not exist
    else:
        flash("Table does not exist")
        return redirect(url_for("adminpage"))

# Creating S3 bucket
@app.route('/createbucket')
def createbucket():
    error = ""

    try:
        s3 = boto3.resource("s3")
        s3.meta.client.head_bucket(Bucket = "musicimg")
    except ClientError as ce:
        error = ce.response["Error"]["Message"]

    # Bucket exists
    if(error != "Not Found"):
        flash("Bucket already exists")
        return redirect(url_for("adminpage"))

    # Bucket does not exist, create it
    else:
        s3_client.create_bucket(Bucket = "musicimg")
        flash("Bucket created")
        return redirect(url_for("adminpage"))

# Fill bucket with images
@app.route('/fillbucket')
def fillbucket():
    error = ""

    try:
        s3 = boto3.resource("s3")
        s3.meta.client.head_bucket(Bucket = "musicimg")
    except ClientError as ce:
        error = ce.response["Error"]["Message"]

    # Bucket exists, fill it with images
    if(error != "Not Found"):
        file_directory = "tmp/images"
        
        # Open the json file in database
        with open("database/a2.json") as json_file:
            music_list = json.load(json_file, parse_float = Decimal)

        musicCounter = 0

        for music in music_list["songs"]:
            ## Create a folder to download the files
            current_directory = os.getcwd()
            final_directory = os.path.join(current_directory, file_directory)

            if not os.path.exists(final_directory):
                os.makedirs(final_directory)

            musicCounter = musicCounter + 1

            music_id_string = str(musicCounter)

            # Store image as image.jpg, in the folder created
            img_url = music["img_url"]
            
            img_data = requests.get(img_url).content
            
            with open(os.path.join(file_directory, "image.jpg"), 'wb') as handler:
                handler.write(img_data)

            # Upload to s3
            filename = music_id_string + ".jpg"

            s3 = boto3.resource("s3")
            s3.Bucket("musicimg").upload_file(os.path.join(final_directory, "image.jpg"), filename, ExtraArgs = {'ACL': 'public-read', 'ContentType': 'image/jpeg'})

        flash("Bucket filled")
        return redirect(url_for("adminpage"))

    # Bucket does not exist
    else:
        flash("Bucket does not exist")
        return redirect(url_for("adminpage"))

# Delete bucket
@app.route('/deletebucket')
def deletebucket():
    error = ""

    try:
        s3 = boto3.resource("s3")
        s3.meta.client.head_bucket(Bucket = "musicimg")
    except ClientError as ce:
        error = ce.response["Error"]["Message"]

    # Bucket exists, delete it
    if(error != "Not Found"):
        bucket = s3.Bucket("musicimg")

        for key in bucket.objects.all():
            key.delete()

        bucket.delete()

        flash("Bucket deleted")
        return redirect(url_for("adminpage"))

    # Bucket does not exist
    else:
        flash("Bucket does not exist")
        return redirect(url_for("adminpage"))

# querypage.html
@app.route('/querypage/<string:email>')
def querypage(email):
    return render_template("querypage.html", email = email)

@app.route('/userquery/<string:email>', methods = ["POST"])
def userquery(email):
    title = request.form.get("title")
    year = request.form.get("year")
    artist = request.form.get("artist")

    titleEmpty = False
    yearEmpty = False
    artistEmpty = False

    if title.strip() == '':
        titleEmpty = True

    if year.strip() == '':
        yearEmpty = True
        
    if artist.strip() == '':
        artistEmpty = True

    # nothing filled 000
    if(titleEmpty and yearEmpty and artistEmpty):
        flash("Nothing filled. Please fill in at least a value")
        return redirect(url_for("querypage", email = email))
    else:
        items = doquery(title, year, artist)

        # Nothing returned
        if(len(items) == 0):
            flash("No result is retrieved. Please query again.")
            return redirect(url_for("querypage", email = email))

        # Something returned. Display query
        else:
            scan_string = title + "," + year + "," + artist
            return redirect(url_for("queryresultpage", email = email, scan_string = scan_string))

def doquery(title, year, artist):
    titleEmpty = False
    yearEmpty = False
    artistEmpty = False

    if title.strip() == '':
        titleEmpty = True

    if year.strip() == '':
        yearEmpty = True
        
    if artist.strip() == '':
        artistEmpty = True

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("music")

    # Scan by - artist 001
    if(titleEmpty and yearEmpty and not artistEmpty):
        response = table.scan(
            FilterExpression = Attr("artist").eq(artist)
        )

    # Scan by - year 010
    elif(titleEmpty and not yearEmpty and artistEmpty):
        response = table.scan(
            FilterExpression = Attr("year").eq(year)
        )

    # # Scan by - year & artist 011
    elif(titleEmpty and not yearEmpty and not artistEmpty):
        response = table.scan(
            FilterExpression = Attr("year").eq(year) & Attr("artist").eq(artist)
        )

    # Scan by - title 100
    elif(not titleEmpty and yearEmpty and artistEmpty):
        response = table.scan(
            FilterExpression = Attr("title").eq(title)
        )

    # Scan by - title & artist 101
    elif(not titleEmpty and yearEmpty and not artistEmpty):
        response = table.scan(
            FilterExpression = Attr("title").eq(title) & Attr("artist").eq(artist)
        )

    # Scan by - title & year 110
    elif(not titleEmpty and not yearEmpty and artistEmpty):
        response = table.scan(
            FilterExpression = Attr("title").eq(title) & Attr("year").eq(year)
        )

    # Scan by - title & year & artist
    elif(not titleEmpty and not yearEmpty and not artistEmpty):
        response = table.scan(
            FilterExpression = Attr("title").eq(title) & Attr("year").eq(year) & Attr("artist").eq(artist)
        )

    items = response["Items"]

    return items


# queryresultpage.html
@app.route('/queryresultpage/<string:email>/<string:scan_string>')
def queryresultpage(email, scan_string):
    scan_list = scan_string.split(",")
    title = scan_list[0]
    year = scan_list[1]
    artist = scan_list[2]

    items = doquery(title, year, artist)

    return render_template("queryresultpage.html", email = email, scan_string = scan_string, items = items)

@app.route('/subscribe/<string:email>/<string:scan_string>', methods = ["POST"])
def subscribe(email, scan_string):
    music_id = request.form.get('music_id')

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("subscribe")

    # Check if user (by email) has already subscribed
    response = table.scan(
        FilterExpression = Attr("email").eq(email) & Attr("music_id").eq(music_id)
    )

    items = response["Items"]

    # User has subscribed already, do not continue
    if(len(items) != 0):
        flash("You are already subscribed.")
        return redirect(url_for("queryresultpage", email = email, scan_string = scan_string))

    # User has not subscribe, add into table
    else:
        table.put_item(
                Item = {
                    "email": email,
                    "music_id": music_id
                }
            )

        flash("Subscribed successful")
        return redirect(url_for("queryresultpage", email = email, scan_string = scan_string))

@app.route('/subscriptionpage/<string:email>')
def subscriptionpage(email):
    dynamodb = boto3.resource("dynamodb")
    subscribe_table = dynamodb.Table("subscribe")

    # Check if user (by email) has any subscriptions
    response = subscribe_table.scan(
        FilterExpression = Attr("email").eq(email)
    )

    subscribe_table_items = response["Items"]

    # Get all music_id and store it in a list
    music_id_list = []

    for item in subscribe_table_items:
        music_id_list.append(item["music_id"])

    # Get all music based on the music_id in the music_id_list
    music_table = dynamodb.Table("music")
    
    items = []
    for music_id in music_id_list:
        response = music_table.scan(
            FilterExpression = Attr("music_id").eq(music_id)
        )

        # Add it to items list, to be returned to the webpage for user to see
        items.append(response["Items"][0])

    return render_template("subscriptionpage.html", email = email, items = items)

@app.route('/removesubscription/<string:email>', methods = ["POST"])
def removesubscription(email):
    music_id = request.form.get('music_id')

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("subscribe")

    response = table.delete_item(
        Key = {
            'email': email,
            'music_id': music_id
        }
    ) 

    flash("Subscription removed")
    return redirect(url_for("subscriptionpage", email = email))

if __name__ == "__main__":
    # For local testing
    app.run(host = "127.0.0.1", port = 8080, debug = True)