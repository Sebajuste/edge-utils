package io.edge.utils.timeseries.influxdb;

import java.util.Map;

import io.edge.utils.timeseries.BatchPoints;
import io.edge.utils.timeseries.Point;

public final class InfluxDBRequest {

	private InfluxDBRequest() {
		super();
	}

	public static String writeRequest(Point point) throws InfluxDBException {

		if (point.getValues().isEmpty()) {
			throw new InfluxDBException("Point has no value");
		}

		final StringBuilder request = new StringBuilder();

		request.append(point.getMeasurement());

		for (Map.Entry<String, String> entry : point.getTags().entrySet()) {
			request.append("," + entry.getKey() + "=" + entry.getValue());
		}

		if (point.getValues().size() > 1) {
			int index = 0;
			request.append(" ");
			for (Map.Entry<String, Object> entry : point.getValues().entrySet()) {
				if (index != 0) {
					request.append(",");
				}
				Object value = entry.getValue();
				if (value instanceof String) {
					request.append(entry.getKey() + "=\"" + entry.getValue().toString() + "\"");
				} else {
					request.append(entry.getKey() + "=" + entry.getValue().toString());
				}
				index++;
			}
		} else {
			Map.Entry<String, Object> entry = point.getValues().entrySet().iterator().next();

			Object value = entry.getValue();

			if (value instanceof String) {
				request.append(" value=\"" + entry.getValue().toString() + "\"");
			} else {
				request.append(" value=" + entry.getValue().toString());
			}
		}

		if (point.haveTimestamp()) {
			request.append(" " + point.getTimestamp());
		}

		return request.toString();
	}

	public static String writeRequest(BatchPoints batch) throws InfluxDBException {

		final StringBuilder request = new StringBuilder();

		int pointsCount = 0;

		int index = 0;
		for (Point point : batch.getPointList()) {

			request.append(InfluxDBRequest.writeRequest(point));
			pointsCount++;

			if (index < batch.getPointList().size()) {
				request.append("\n");
			}

			index++;
		}

		if (pointsCount == 0) {
			throw new InfluxDBException("Batch has no point");
		}

		return request.toString();
	}

}
