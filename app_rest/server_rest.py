#
#  Servico RESTAPI Employees
#
# -*- coding: utf-8 -*-
# !/usr/bin/python3

from pyspark.sql import SparkSession

from flask import Flask
from flask_restful import Resource, Api
import json

import os
from flask import send_from_directory

app = Flask(__name__)
api = Api(app)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico',
                               mimetype='image/vnd.microsoft.icon')


class Concorrentes(Resource):
    _spark = None

    def __init__(self):
        # Iniciar spark session
        self._spark = SparkSession.builder \
            .master("local") \
            .appName("GeoFusion CASE") \
            .getOrCreate()

    def get(self, concorrente_id):
        result = {}

        try:
            df_concorrente = self._spark.read.option("encoding", "utf-8").csv('hive_final', header=True,
                                                                              sep='|')

            df_concorrente = df_concorrente.where('codigo_concorrente ==' + concorrente_id)

            result = df_concorrente.toJSON().map(lambda j: json.loads(j)).collect()
        except:
            result = {'Error'}
        finally:
            return result


# Endpoint para as chamadas do servico
api.add_resource(Concorrentes, '/concorrentes/<concorrente_id>')  # Route_1

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
