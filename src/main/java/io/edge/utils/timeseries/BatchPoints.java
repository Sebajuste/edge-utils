package io.edge.utils.timeseries;

import java.util.Collections;
import java.util.List;

public class BatchPoints {

	protected final String dbName;

	protected final List<Point> pointList;

	public BatchPoints(String dbName, List<Point> pointList) {
		super();
		this.dbName = dbName;
		this.pointList = Collections.unmodifiableList(pointList);
	}

	public static BatchPointsBuilder database(String dbName) {
		return new BatchPointsBuilder(dbName);
	}

	public String getDbName() {
		return dbName;
	}

	public List<Point> getPointList() {
		return pointList;
	}

	public boolean isEmpty() {
		return pointList.isEmpty();
	}
	
	@Override
	public String toString() {
		return "BatchPoints [dbName=" + dbName + ", pointList=" + pointList + "]";
	}

}
