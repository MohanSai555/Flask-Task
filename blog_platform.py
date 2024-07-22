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
    # print(conn)
    return conn

# Table Creation


""" CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    post_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


class BlogPost(Resource):
    def post(self):
        conn = False
        cursor = False
        try:
            conn = con()
            data = request.get_json()
            query = f"INSERT INTO blog_posts (title, content, author, created_at)" \
                    f" VALUES ('{data['title']}','{data['content']}','{data['author']}',sysdate)"
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return {"res_status": True, "status": 200, 'message': 'Blog post created successfully'}
        except Exception as e:
            logging.error("Error occurred while creating blog post: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                cursor.close()
                conn.close()

    def get(self, post_id):
        conn = False
        try:
            conn = con()
            query = f"SELECT * FROM blog_posts WHERE post_id = {post_id}"
            df = pd.read_sql(query, conn)
            post_data = df.to_dict(orient='records')
            if post_data:
                return {"res_status": True, "status": 200, 'data': post_data}
            else:
                return {"res_status": True, "status": 200, 'message': 'Blog post not found'}
        except Exception as e:
            logging.error("Error occurred while fetching blog post: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                conn.close()


class AllBlogPosts(Resource):
    def get(self):
        conn = False
        try:
            conn = con()
            query = "SELECT * FROM blog_posts"
            df = pd.read_sql(query, conn)
            posts = df.to_dict(orient='records')
            return {"res_status": True, "status": 200, 'data': posts}
        except Exception as e:
            logging.error("Error occurred while fetching blog posts: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                conn.close()


class BlogPostComments(Resource):
    def post(self, post_id):
        conn = False
        cursor = False
        try:
            conn = con()
            data = request.get_json()
            query = f"INSERT INTO comments (post_id, author, content, created_at) " \
                    f"VALUES ({post_id}, '{data['author']}','{data['content']}', sysdate)"
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return {"res_status": True, "status": 200, 'message': 'Comment added successfully'}
        except Exception as e:
            logging.error("Error occurred while adding comment: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                cursor.close()
                conn.close()

    def get(self, post_id):
        conn = False
        try:
            conn = con()
            query = f"SELECT * FROM comments WHERE post_id = {post_id}"
            df = pd.read_sql(query, conn)
            comments = df.to_dict(orient='records')
            return {"res_status": True, "status": 200, 'data': comments}
        except Exception as e:
            logging.error("Error occurred while fetching comments: " + str(e))
            return {"res_status": False, 'message': str(e)}
        finally:
            if str(conn) == 'True':
                conn.close()


# API resource routing
api.add_resource(BlogPost, '/blogpost', '/blogpost/<int:post_id>')
api.add_resource(AllBlogPosts, '/blogposts')
api.add_resource(BlogPostComments, '/blogpost/<int:post_id>/comments')

if __name__ == '__main__':
    app.run(debug=True)
