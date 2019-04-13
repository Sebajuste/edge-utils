package io.edge.utils.timeseries.influxdb;

public class InfluxDBException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InfluxDBException() {
		super();
	}

	public InfluxDBException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InfluxDBException(String message, Throwable cause) {
		super(message, cause);
	}

	public InfluxDBException(String message) {
		super(message);
	}

	public InfluxDBException(Throwable cause) {
		super(cause);
	}

}
