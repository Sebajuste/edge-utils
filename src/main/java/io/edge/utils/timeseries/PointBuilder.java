package io.edge.utils.timeseries;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.vertx.core.json.JsonObject;

public class PointBuilder {

	private static final String DEFAULT_VALUE_NAME = "value";

	private final String measurement;

	private final Map<String, String> tags = new HashMap<>();

	private final Map<String, Object> values = new HashMap<>();

	private boolean haveTimestamp = false;

	private TimeUnit timeunit;

	private long timestamp;

	protected PointBuilder(String measurement) {
		this.measurement = measurement;
	}

	public PointBuilder addTag(String name, String value) {
		tags.put(name, value);
		return this;
	}

	protected PointBuilder addValue(String name, Object value) {
		values.put(name, value);
		return this;
	}

	public PointBuilder addValue(String name, String value) {
		values.put(name, value);
		return this;
	}

	public PointBuilder addValue(String name, float value) {
		values.put(name, value);
		return this;
	}

	public PointBuilder addValue(String name, int value) {
		values.put(name, value);
		return this;
	}

	public PointBuilder addValue(String name, boolean value) {
		values.put(name, value);
		return this;
	}

	public PointBuilder setValue(String value) {
		values.put(DEFAULT_VALUE_NAME, value);
		return this;
	}

	public PointBuilder setValue(float value) {
		values.put(DEFAULT_VALUE_NAME, value);
		return this;
	}

	public PointBuilder setValue(int value) {
		values.put(DEFAULT_VALUE_NAME, value);
		return this;
	}

	public PointBuilder setValue(boolean value) {
		values.put(DEFAULT_VALUE_NAME, value);
		return this;
	}

	/**
	 * Value must be a primitive type like Integer, Float, Boolean or String
	 * 
	 * @param value
	 * @return
	 */
	public PointBuilder setValue(Object value) {
		values.put(DEFAULT_VALUE_NAME, value);
		return this;
	}

	public PointBuilder time(long timestamp, TimeUnit timeunit) {
		this.timestamp = timestamp;
		this.timeunit = timeunit;
		this.haveTimestamp = true;
		return this;
	}

	public Point build() {
		if (haveTimestamp) {
			return new Point(measurement, timestamp, timeunit, Collections.unmodifiableMap(tags), Collections.unmodifiableMap(values));
		} else {
			return new Point(measurement, Collections.unmodifiableMap(tags), Collections.unmodifiableMap(values));
		}
	}

	public JsonObject toJson() {
		return this.build().toJson();
	}

}
