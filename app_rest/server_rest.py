#
#  Servico RESTAPI Employees
#
# -*- coding: utf-8 -*-
# !/usr/bin/python3

import pandas as pd

from flask import Flask
from flask_restful import Resource, Api

import os
from flask import send_from_directory

app = Flask(__name__)
api = Api(app)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico',
                               mimetype='image/vnd.microsoft.icon')


class Concorrentes(Resource):
    def get(self, concorrente_id):
        result = {}

        try:
            df_concorrente = pd.read_csv("final_concorrentes.csv", delimiter="|", encoding='utf8')

            df_concorrente = df_concorrente.query('codigo_concorrente ==' + concorrente_id )

            result = df_concorrente.to_json()
        except:
            result = {'Error'}
        finally:
            return result


# Endpoint para as chamadas do servico
api.add_resource(Concorrentes, '/concorrentes/<concorrente_id>')  # Route_1

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')