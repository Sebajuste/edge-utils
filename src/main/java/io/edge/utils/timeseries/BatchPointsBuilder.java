package io.edge.utils.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class BatchPointsBuilder {

	protected final String dbName;

	protected final List<Point> pointList = new ArrayList<>();

	protected BatchPointsBuilder(String dbName) {
		super();
		this.dbName = dbName;
	}

	public BatchPointsBuilder points(Collection<? extends Point> points) {
		this.pointList.addAll(points);
		return this;
	}

	public BatchPointsBuilder points(Point... points) {
		pointList.addAll(Arrays.asList(points));
		return this;
	}

	public BatchPoints build() {
		return new BatchPoints(dbName, Collections.unmodifiableList(pointList));
	}

}
