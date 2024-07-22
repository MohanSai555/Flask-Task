import logging
import cx_Oracle
from flask import Flask, request
from flask_cors import CORS
from flask_restful import Api, Resource
import pandas as pd

# Initialize Flask app and extensions
app = Flask(__name__)
api = Api(app)
CORS(app)

# Configure pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

# Database connection function


def con():
    conn = cx_Oracle.connect('user/password@host:port/dbname', encoding="UTF-8")
    # print(conn)
    return conn

# Table Creation


""""CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


class User(Resource):
    def post(self):
        conn = False
        cursor = False
        try:
            # Connect to Oracle database
            conn = con()
            data = request.get_json()
            query = f"INSERT INTO users (username, email) VALUES ('{data['username']}', '{data['email']}')"
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return {"res_status": True, "status": 200, 'message': 'User created successfully'}
        except Exception as e:
            logging.error("Error occurred while creating user: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                cursor.close()
                conn.close()

    def get(self, seq_id):
        conn = False
        try:
            # Connect to Oracle database
            conn = con()
            query = f"SELECT * FROM users WHERE id = {0}"
            df = pd.read_sql(query, conn).format(seq_id)
            user_data = df.to_dict(orient='records')
            if user_data:
                return {"res_status": True, "status": 200, 'data': user_data}
            else:
                return {"res_status": True, "status": 200, 'message': 'User not found'}
        except Exception as e:
            logging.error("Error occurred while retrieving user: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                conn.close()


class AllUsers(Resource):
    def get(self):
        conn = False
        try:
            # Connect to Oracle database
            conn = con()
            query = "SELECT * FROM users"
            df = pd.read_sql(query, conn)
            users_data = df.to_dict(orient='records')
            return {"res_status": True, "status": 200, 'data': users_data}
        except Exception as e:
            logging.error("Error occurred while retrieving users: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                conn.close()


api.add_resource(User, '/users', '/users/<int:id>')
api.add_resource(AllUsers, '/users/all')

if __name__ == '__main__':
    app.run(debug=True)
