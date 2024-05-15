from concurrent.futures import ProcessPoolExecutor,ThreadPoolExecutor
from config.db_port import get_database
from logs import logs_config
import numpy as np
import concurrent

db = get_database()
transformed_data = db.transformed_data
sensor = db.load_profile_jdvvnl

def data_fetch(sensor_id):
    try:
        fromId = sensor_id + "-2024-01-01 00:00:00"
        toId = sensor_id + "-2024-03-31 23:59:59"
        query = {"_id": {"$gte": fromId, "$lt": toId}}
        results = np.array([doc for doc in sensor.find(query)])
        
        if results.size > 0 and results.dtype.names is not None:
            id_index = results.dtype.names.index('_id')
            results[id_index] = results[id_index].astype(str)

        
        logs_config.logger.info(f"Fetched {len(results)} documents for sensor_id: {sensor_id}")
        return results
    except Exception as e:
        logs_config.logger.error(f"Error fetching data for sensor_id {sensor_id}:", exc_info=True)
        return np.array([])

def fetch_data_for_sensors(sensor_ids=["f3687b47-9fdd-4699-8d73-251637c953f1","f2d85cdc-63e1-4168-b3e6-7acc712de566"] , batch_size = 100):

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(data_fetch, sensor_id): sensor_id for sensor_id in sensor_ids}

    results = []
    for future in concurrent.futures.as_completed(futures):
        sensor_id = futures[future]
        try:
            results.append(future.result()) 
        except Exception as exc:
            logs_config.logger.error(f"Error fetching data for sensor_id {sensor_id}: {exc}")
    
    return results
