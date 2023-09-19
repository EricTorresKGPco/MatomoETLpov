from flask import Flask, jsonify, request
from flask_restful import Resource, Api
import matomo_etl as etl

app = Flask(__name__)
api = Api(app)

class ETL(Resource):
  
    # corresponds to the GET request.
    # this function is called whenever there
    # is a GET request for this resource
    def get(self):
  
        df = etl.main()
        return jsonify({'data': df}) 
        

api.add_resource(ETL, '/')
