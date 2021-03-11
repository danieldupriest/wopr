--Visualization 1. A visualization of speeds for a single trip for any bus route that crosses the Glenn Jackson I-205 bridge.

SELECT * FROM trip, breadcrumb
WHERE trip.trip_id = breadcrumb.trip_id
  AND latitude > 45.586158
  AND latitude < 45.592404
  AND longitude > -122.550711
  AND longitude < -122.541270
LIMIT 5;

--Let’s use trip 169422462

\COPY (
  SELECT speed, latitude, longitude
  FROM breadcrumb
  WHERE trip_id = 169422462
) TO 'output.tsv';

--Visualization 2. All outbound trips that occurred on route 65 on any Friday (you choose which Friday) between the hours of 4pm and 6pm.

--Outbound trips on route 65 on Friday the 16th, October between 4pm and 6pm.

SELECT * FROM trip, breadcrumb
WHERE breadcrumb.trip_id = trip.trip_id
AND route_id = 65
AND trip.direction = 'Out'
AND tstamp > '2020-10-16 16:00:00'
AND tstamp < '2020-10-16 18:00:00';

\COPY (
  SELECT speed, latitude, longitude
  FROM trip, breadcrumb
  WHERE breadcrumb.trip_id = trip.trip_id
  AND route_id = 65
  AND trip.direction = 'Out'
  AND tstamp > '2020-10-16 16:00:00'
  AND tstamp < '2020-10-16 18:00:00'
) TO 'output.tsv';

--Visualization 3. All outbound trips for route 65 on any Sunday morning (you choose which Sunday) between 9am and 11am.

--Outbound trips for route 65 on Sunday the 18th, October between 9am and 11am

\COPY (
  SELECT speed, latitude, longitude
  FROM trip, breadcrumb
  WHERE breadcrumb.trip_id = trip.trip_id
  AND route_id = 65
  AND trip.direction = 'Out'
  AND tstamp > '2020-10-18 09:00:00'
  AND tstamp < '2020-10-18 11:00:00'
) TO 'output.tsv';

--Visualization 4. The longest (as measured by time) trip in your entire data set. Indicate the date, route #, and trip ID of the trip along with a visualization showing the entire trip.

SELECT MAX(tstamp) - MIN(tstamp) AS duration,
  MIN(tstamp) AS date,
  MIN(trip.trip_id) AS trip_id,
  MIN(route_id) AS route_id
FROM breadcrumb, trip
WHERE breadcrumb.trip_id = trip.trip_id
GROUP BY trip.trip_id
ORDER BY duration DESC;

--5a. Speeds for a route that goes through downtown Vancouver. This is to visualize slowdown in the denser parts of town.

SELECT DISTINCT route_id
FROM trip, breadcrumb
WHERE breadcrumb.trip_id = trip.trip_id
AND latitude > 45.6311
AND latitude < 45.6399
AND longitude > -122.6773
AND longitude < -122.6605;

--This gets us 12 routes going through downtown. Let’s map route 25.

\COPY (
  SELECT speed, latitude, longitude
  FROM trip, breadcrumb
  WHERE breadcrumb.trip_id = trip.trip_id
  AND route_id = 25
) TO 'output.tsv';

--5b. Route with southernmost reach. This is to visualize slowdown in PDX area.

SELECT latitude, route_id
FROM trip, breadcrumb
WHERE breadcrumb.trip_id = trip.trip_id
AND route_id != 0
ORDER BY latitude;

--Let’s map route 190, which has the lowest latitude (of the breadcrumbs connected to stop data)

\COPY (
  SELECT speed, latitude, longitude
  FROM trip, breadcrumb
  WHERE breadcrumb.trip_id = trip.trip_id
  AND route_id = 190
) TO 'output.tsv';

--5c. Speed of a bus route crossing the I5 bridge.

SELECT DISTINCT route_id
FROM trip, breadcrumb
WHERE breadcrumb.trip_id = trip.trip_id
AND latitude > 45.61613
AND latitude < 45.61859
AND longitude > -122.67756
AND longitude < -122.67191;

--Let’s map bus 105.

\COPY (
  SELECT speed, latitude, longitude
  FROM trip, breadcrumb
  WHERE breadcrumb.trip_id = trip.trip_id
  AND route_id = 105
) TO 'output.tsv';