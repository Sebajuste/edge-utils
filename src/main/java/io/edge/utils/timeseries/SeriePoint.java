package io.edge.utils.timeseries;

public class SeriePoint {

	private final Object value;

	private final long timestamp;

	public SeriePoint(Object value, long timestamp) {
		super();
		this.value = value;
		this.timestamp = timestamp;
	}

	public Object getValue() {
		return value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isEmpty() {
		return this.value == null;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		SeriePoint other = (SeriePoint) obj;
		if (timestamp != other.timestamp) return false;
		return true;
	}

	@Override
	public String toString() {
		return "SeriePoint [value=" + value + ", timestamp=" + timestamp + "]";
	}

}
