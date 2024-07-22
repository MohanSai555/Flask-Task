import logging
import cx_Oracle
import pandas as pd
from flask import Flask, request
from flask_cors import CORS
from flask_restful import Api, Resource

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
    return conn

# Table Creation
""" CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    total_price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
"""


class OrderList(Resource):
    def post(self):
        conn = False
        cursor = False
        try:
            conn = con()
            data = request.get_json()
            query = "INSERT INTO orders (user_id, created_at) VALUES (:1, sysdate) RETURNING order_id INTO :2"
            cursor = conn.cursor()
            order_id = cursor.var(cx_Oracle.NUMBER)
            cursor.execute(query, [data['user_id'], order_id])
            conn.commit()
            return {"res_status": True, "status": 200, 'order_id': int(order_id.getvalue()[0])}
        except Exception as e:
            logging.error("Error occurred while creating order: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                cursor.close()
                conn.close()


class OrderItemList(Resource):
    def post(self, order_id):
        conn = False
        cursor = False
        try:
            conn = con()
            data = request.get_json()
            query = "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (:1, :2, :3, :4)"
            cursor = conn.cursor()
            cursor.execute(query, [order_id, data['product_id'], data['quantity'], data['price']])
            conn.commit()
            self.update_order_total(order_id)
            return {"res_status": True, "status": 200, 'message': 'Item added to order successfully'}
        except Exception as e:
            logging.error("Error occurred while adding item to order: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                cursor.close()
                conn.close()

    def update_order_total(self, order_id):
        conn = False
        cursor = False
        try:
            conn = con()
            cursor = conn.cursor()
            total_query = f"SELECT SUM(quantity * price) as total_price FROM order_items WHERE order_id = {order_id}"
            cursor.execute(total_query)
            total_price = cursor.fetchone()[0]
            update_query = f"UPDATE orders SET total_price = {total_price} WHERE order_id = {order_id}"
            cursor.execute(update_query)
            conn.commit()
        except Exception as e:
            logging.error("Error occurred while updating order total: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                cursor.close()
                conn.close()


class OrderTotal(Resource):
    def get(self, order_id):
        conn = False
        try:
            conn = con()
            query = f"SELECT order_id, total_price FROM orders WHERE order_id = {0}"
            df = pd.read_sql(query, conn).format(order_id)
            order_data = df.to_dict(orient='records')
            if order_data:
                return {"res_status": True, "status": 200, 'data': order_data}
            else:
                return {"res_status": True, "status": 200, 'message': 'Order not found'}
        except Exception as e:
            logging.error("Error occurred while fetching order total: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                conn.close()



# API resource routing

api.add_resource(OrderList, '/orders')
api.add_resource(OrderItemList, '/orders/<int:id>/items')
api.add_resource(OrderTotal, '/orders/<int:id>/total')

if __name__ == '__main__':
    app.run(debug=True)
