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

# Table creation


""" CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL
); """


class Product(Resource):
    def post(self):
        conn = False
        cursor = False
        try:
            # Connect to Oracle database
            conn = con()
            data = request.get_json()
            query = f"INSERT INTO products (name, quantity, price) VALUES ('{data['name']}'," \
                    f"{data['quantity']}, {data['price']})"
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return {"res_status": True, "status": 200, 'message': 'Product added successfully'}
        except Exception as e:
            logging.error("Error occurred while adding product: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                cursor.close()
                conn.close()

    def put(self, seq_id):
        conn = False
        cursor = False
        try:
            # Connect to Oracle database
            conn = con()
            data = request.get_json()
            query = f"UPDATE products SET quantity = {data['quantity']} WHERE id = {seq_id}"
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return {"res_status": True, "status": 200, 'message': 'Product quantity updated successfully'}
        except Exception as e:
            logging.error("Error occurred while updating product: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                cursor.close()
                conn.close()


class InventoryValue(Resource):
    def get(self):
        conn = False
        try:
            # Connect to Oracle database
            conn = con()
            query = "SELECT SUM(quantity * price) AS total_inventory_value FROM products"
            df = pd.read_sql(query, conn)
            total_value = df['total_inventory_value'].iloc[0]
            return {"res_status": True, "status": 200, 'total_inventory_value': total_value}
        except Exception as e:
            logging.error("Error occurred while calculating inventory value: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                conn.close()


api.add_resource(Product, '/products', '/products/<int:id>')
api.add_resource(InventoryValue, '/inventory/value')

if __name__ == '__main__':
    app.run(debug=True)
