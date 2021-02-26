
JSON_COLUMNS = ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'VELOCITY', 'DIRECTION', 'RADIO_QUALITY', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION']
SELECTED_HDRS = ['EVENT_NO_TRIP', 'OPD_DATE', 'VEHICLE_ID', 'ACT_TIME', 'VELOCITY', 'DIRECTION', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'num_rows', 'json_date']
TO_DROP = ['EVENT_NO_STOP','METERS', 'RADIO_QUALITY', 'GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION']
FINAL_COLS = ['EVENT_NO_TRIP', 'VEHICLE_ID', 'VELOCITY', 'DIRECTION', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'tstamp']

VEHICLE_IDS = [2267, 2271, 2284, 2285, 2287, 2289, 2290, 2401, 2404, 4002, 4003, 4004, 4006, 4007, 4008, 4009, 4011, 4012, 4013, 4015, 4017, 4018, 4019, 4031, 4032, 4036, 4037, 4038, 6003, 6006, 6008, 6009, 6010, 1298380, 2268, 4010, 4014, 4016, 4034, 6005, 2262, 2263, 2286, 4020, 6001, 1776, 2218, 2220, 2223, 2224, 2226, 2227, 2228, 2231, 2232, 2233, 2235, 2237, 2238, 2239, 2240, 2241, 2242, 2243, 2244, 2245, 2246, 2247, 2248, 2249, 2250, 2264, 2265, 2266, 2288, 2291, 2292, 2293, 2294, 2403, 2901, 2902, 4005, 4025, 4026, 4027, 4028, 4029, 4030, 4033, 6002, 6004, 1254260, 1254280, 1254282, 1254300, 2251, 2269, 2402, 4001, 2270, 6007, 2215]

STOP_HDRS = ['vehicle_number','leave_time','train','route_number','direction','service_key','stop_time','arrive_time','dwell','location_id','door','lift','ons','offs','estimated_load','maximum_speed','train_mileage','pattern_distance','location_distance','x_coordinate','y_coordinate','data_source','schedule_status','trip_id']

STOP_HDRS_TO_KEEP = ['vehicle_number','route_number','direction','service_key','trip_id']

STOP_HDRS_TO_DROP = ['leave_time','train','stop_time','arrive_time','dwell','location_id','door','lift','ons','offs','estimated_load','maximum_speed','train_mileage','pattern_distance','location_distance','x_coordinate','y_coordinate','data_source','schedule_status']
