from Mongo_db import data ,sensor_ids
from flask import Blueprint,jsonify,request
from logs import logs_config

data_bp= Blueprint('data_fetch',__name__)

@data_bp.route("/data",methods = ['POST','GET'])
def data_find():
    logs_config.logger.info("Data Fetching")
    circle_id = request.args.get('circle_id')
    # sensors = sensor_ids.sensor_ids(circle_id)
    sensors = request.args.get("sensors")
    
    results = data.fetch_data_for_sensors(sensors)
    

    return jsonify(results)