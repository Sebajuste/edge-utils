package io.edge.utils.timeseries;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.vertx.core.json.JsonObject;

public class Point {

	private static final String HAVE_TIMESTAMP = "haveTimestamp";

	protected final String measurement;

	protected final long timestamp;

	protected final TimeUnit timeunit;

	protected final boolean haveTimestamp;

	protected final Map<String, String> tags;

	protected final Map<String, Object> values;

	public Point(String measurement, long timestamp, TimeUnit timeunit, Map<String, String> tags, Map<String, Object> values) {
		super();
		this.measurement = measurement;
		this.timestamp = timestamp;
		this.haveTimestamp = true;
		this.timeunit = timeunit;
		this.tags = tags;
		this.values = values;
	}

	public Point(String measurement, Map<String, String> tags, Map<String, Object> values) {
		super();
		this.measurement = measurement;
		this.haveTimestamp = false;
		this.timeunit = null;
		this.timestamp = 0;
		this.tags = tags;
		this.values = values;
	}

	public static PointBuilder measurement(String measurement) {
		return new PointBuilder(measurement);
	}

	public static PointBuilder measurement(JsonObject measure) {
		PointBuilder pointBuilder = new PointBuilder(measure.getString("measurement"));

		if (measure.containsKey(HAVE_TIMESTAMP) && measure.getBoolean(HAVE_TIMESTAMP)) {
			pointBuilder.time(measure.getLong("timestamp"), TimeUnit.valueOf(measure.getString("timeunit")));
		}

		JsonObject tags = measure.getJsonObject("tags");

		for (String name : tags.fieldNames()) {
			pointBuilder.addTag(name, tags.getString(name));
		}

		JsonObject values = measure.getJsonObject("values");

		for (String name : values.fieldNames()) {
			pointBuilder.addValue(name, values.getValue(name));
		}

		return pointBuilder;
	}

	public String getMeasurement() {
		return measurement;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public TimeUnit getTimeunit() {
		return timeunit;
	}

	public boolean haveTimestamp() {
		return haveTimestamp;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public Map<String, Object> getValues() {
		return values;
	}

	public JsonObject toJson() {

		JsonObject localTags = new JsonObject();

		for (Map.Entry<String, String> entry : this.tags.entrySet()) {
			localTags.put(entry.getKey(), entry.getValue());
		}

		JsonObject localValues = new JsonObject();

		for (Map.Entry<String, Object> entry : this.values.entrySet()) {
			localValues.put(entry.getKey(), entry.getValue());
		}

		JsonObject measure = new JsonObject();
		measure.put("measurement", measurement);
		measure.put("timestamp", timestamp);
		measure.put("timeunit", timeunit);
		measure.put(HAVE_TIMESTAMP, haveTimestamp);
		measure.put("tags", localTags);
		measure.put("values", localValues);

		return measure;
	}

	@Override
	public int hashCode() {
		return Objects.hash(measurement, timestamp, timeunit, haveTimestamp, tags, values);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}

		if (obj instanceof Point) {
			Point other = (Point) obj;

			return Objects.equals(measurement, other.measurement) && Objects.equals(timestamp, other.timestamp) && Objects.equals(timeunit, other.timeunit) && Objects.equals(haveTimestamp, other.haveTimestamp) && Objects.equals(tags, other.tags) && Objects.equals(values, other.values);
		}

		return false;

	}

	@Override
	public String toString() {
		return "Point [measurement=" + measurement + ", timestamp=" + timestamp + ", timeunit=" + timeunit + ", haveTimestamp=" + haveTimestamp + ", tags=" + tags + ", values=" + values + "]";
	}

}
