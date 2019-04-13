package io.edge.utils.timeseries;

import java.util.Collections;
import java.util.List;

import io.vertx.core.json.JsonObject;

public class Serie {

	private final String measurement;

	private final JsonObject tags;

	private final List<SeriePoint> points;

	public Serie(String measurement, JsonObject tags, List<SeriePoint> points) {
		super();
		this.measurement = measurement;
		this.tags = tags;
		this.points = Collections.unmodifiableList(points);
	}

	public String getMeasurement() {
		return measurement;
	}

	public JsonObject getTags() {
		return tags;
	}

	public List<SeriePoint> getPoints() {
		return points;
	}

	@Override
	public String toString() {
		return "Serie [measurement=" + measurement + ", tags=" + tags + ", points=" + points + "]";
	}

}
