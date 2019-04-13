package io.edge.utils.timeseries.influxdb;

public class InfluxDbOptions {

	private String host = "localhost";

	private int port = 8086;

	private String userName;

	private String password;

	public InfluxDbOptions credentials(String userName, String password) {
		this.userName = userName;
		this.password = password;
		return this;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public InfluxDbOptions setUserName(String userName) {
		this.userName = userName;
		return this;
	}

	public String getPassword() {
		return password;
	}

	public InfluxDbOptions setPassword(String password) {
		this.password = password;
		return this;
	}

	@Override
	public String toString() {
		return "Options [userName=" + userName + ", password=" + password + "]";
	}

}
