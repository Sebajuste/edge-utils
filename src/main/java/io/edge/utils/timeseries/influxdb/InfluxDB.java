package io.edge.utils.timeseries.influxdb;

import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import io.edge.utils.sql.SQLBuilder;
import io.edge.utils.sql.SQLSelectBuilder;
import io.edge.utils.timeseries.BatchPoints;
import io.edge.utils.timeseries.Point;
import io.edge.utils.timeseries.Serie;
import io.edge.utils.timeseries.SeriePoint;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class InfluxDB {

	private final WebClient client;

	private final InfluxDbOptions options;

	protected InfluxDB(WebClient client, InfluxDbOptions options) {
		super();
		this.client = client;
		this.options = options;
	}

	public static InfluxDB connect(WebClient client) {
		return new InfluxDB(client, new InfluxDbOptions());
	}

	public static InfluxDB connect(WebClient client, InfluxDbOptions options) {
		return new InfluxDB(client, options);
	}

	public static InfluxDB connect(HttpClient client) {
		return new InfluxDB(WebClient.wrap(client), new InfluxDbOptions());
	}

	public static InfluxDB connect(HttpClient client, InfluxDbOptions options) {
		return new InfluxDB(WebClient.wrap(client), options);
	}

	public static InfluxDB create(Vertx vertx, InfluxDbOptions options) {

		HttpClientOptions httpOptions = new HttpClientOptions();
		httpOptions.setDefaultHost(options.getHost());
		httpOptions.setDefaultPort(options.getPort());

		HttpClient client = vertx.createHttpClient(httpOptions);

		return InfluxDB.connect(client, options);
	}

	private void getRequest(String requestURI, JsonObject queryParams, Handler<AsyncResult<Buffer>> handler) {

		final Future<Buffer> future = Future.future();
		future.setHandler(handler);

		HttpRequest<Buffer> httpRequest = client.get(requestURI);

		if (options.getUserName() != null && options.getUserName().trim().length() > 0) {
			httpRequest.basicAuthentication(options.getUserName(), options.getPassword());
		}

		for (String field : queryParams.fieldNames()) {
			httpRequest.addQueryParam(field, queryParams.getValue(field).toString());
		}

		httpRequest.send(ar -> {
			if (ar.succeeded()) {

				HttpResponse<Buffer> response = ar.result();

				Buffer body = response.body();

				if (response.statusCode() == HttpResponseStatus.OK.code()) {
					future.complete(body);
				} else if (response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
					future.complete();
				} else {

					if ("Not series found".equalsIgnoreCase(body.toString())) {
						future.complete();
					} else {
						future.fail(body.toString());
					}
				}

			} else {
				future.fail(ar.cause());
			}
		});

	}

	private void postRequest(String requestURI, JsonObject queryParams, Buffer chunk, Handler<AsyncResult<Void>> handler) {

		final Future<Void> future = Future.future();

		future.setHandler(handler);

		HttpRequest<Buffer> httpRequest = client.post(requestURI);

		if (options.getUserName() != null && options.getUserName().trim().length() > 0) {
			httpRequest.basicAuthentication(options.getUserName(), options.getPassword());
		}

		httpRequest.putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/x-www-form-urlencoded");

		for (String field : queryParams.fieldNames()) {
			httpRequest.addQueryParam(field, queryParams.getValue(field).toString());
		}
		
		httpRequest.sendBuffer(chunk, ar -> {

			if (ar.succeeded()) {

				HttpResponse<Buffer> response = ar.result();

				if (response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
					future.complete();
				} else {
					future.fail(response.body().toString());
				}

			} else {
				future.fail(ar.cause());
			}

		});

	}

	public void createDatabase(String dbName, Handler<AsyncResult<Void>> handler) {

		Future<Void> future = Future.future();
		future.setHandler(handler);

		JsonObject params = new JsonObject()//
				.put("q", "CREATE DATABASE \"" + dbName + "\"");

		this.getRequest("/query", params, ar -> {
			if (ar.succeeded()) {
				future.complete();
			} else {
				future.fail(ar.cause());
			}
		});

	}

	public void deleteDatabase(String dbName, Handler<AsyncResult<Void>> handler) {
		Future<Void> future = Future.future();
		future.setHandler(handler);

		JsonObject params = new JsonObject()//
				.put("q", "DELETE DATABASE \"" + dbName + "\"");

		this.getRequest("/query", params, ar -> {
			if (ar.succeeded()) {
				future.complete();
			} else {
				future.fail(ar.cause());
			}
		});

	}

	public void write(String dbName, Point point, Handler<AsyncResult<Void>> handler) {
		try {

			JsonObject params = new JsonObject()//
					.put("db", dbName)//
					.put("precision", "ms");

			this.postRequest("/write", params, InfluxDBRequest.writeRequest(point), handler);
		} catch (Exception e) {
			handler.handle(Future.failedFuture(e));
		}
	}

	public void write(BatchPoints batch, Handler<AsyncResult<Void>> handler) {
		try {

			JsonObject params = new JsonObject()//
					.put("db", batch.getDbName())//
					.put("precision", "ms");

			this.postRequest("/write", params, InfluxDBRequest.writeRequest(batch), handler);

		} catch (Exception e) {
			handler.handle(Future.failedFuture(e));
		}
	}

	public void query(String dbName, String query, Handler<AsyncResult<JsonArray>> resultHandler) {

		Future<JsonArray> future = Future.future();
		future.setHandler(resultHandler);

		try {

			JsonObject params = new JsonObject()//
					.put("db", dbName)//
					.put("q", URLEncoder.encode(query, "UTF-8"));

			this.getRequest("/query", params, ar -> {

				if (ar.succeeded()) {

					try {

						JsonObject body = new JsonObject(ar.result());

						JsonArray resultSeries = body.getJsonArray("results").getJsonObject(0).getJsonArray("series");

						if (resultSeries == null) {
							future.fail("Not series found");
						} else {
							future.complete(resultSeries);
						}

					} catch (Exception e) {
						future.fail(e);
					}

				} else {
					future.fail(ar.cause());
				}

			});
		} catch (Exception e) {
			future.fail(e);
		}

	}

	public void querySerie(String dbName, String measurement, LocalDateTime startDateTime, JsonObject tags, String timeGroup, Handler<AsyncResult<Serie>> resultHandler) {
		this.querySerie(dbName, measurement, startDateTime, null, tags, timeGroup, resultHandler);
	}

	public void querySerie(String dbName, String measurement, LocalDateTime startDateTime, LocalDateTime endDateTime, JsonObject tags, String timeGroup, Handler<AsyncResult<Serie>> resultHandler) {
		this.querySerie(dbName, measurement, startDateTime, endDateTime, ZoneId.systemDefault(), tags, timeGroup, resultHandler);
	}

	public void querySerie(String dbName, String measurement, LocalDateTime startDateTime, LocalDateTime endDateTime, ZoneId zone, JsonObject tags, String timeGroup, Handler<AsyncResult<Serie>> resultHandler) {

		String startDateTimeISO = DateTimeFormatter.ISO_INSTANT.format(startDateTime.atZone(zone).toInstant());

		SQLSelectBuilder queryBuilder = SQLBuilder.createSelect()//
				.select("mean(\"value\") as \"mean_value\"")//
				.from("\"" + measurement + "\"")//
				.where("time >= '" + startDateTimeISO + "'")//
		;

		if (endDateTime != null) {
			String endDateTimeISO = DateTimeFormatter.ISO_INSTANT.format(endDateTime.atZone(zone).toInstant());
			queryBuilder.and("time < '" + endDateTimeISO + "'");
		}

		for (String key : tags.fieldNames()) {
			queryBuilder.and(key + " = '" + tags.getString(key) + "'");
		}

		String query = queryBuilder.groupBy("time(" + timeGroup + ") FILL(null)")//
				.build();

		Future<Serie> future = Future.future();
		future.setHandler(resultHandler);

		this.query(dbName, query, ar -> {

			if (ar.succeeded()) {

				JsonArray series = ar.result();

				try {

					JsonObject resultSerie = series.getJsonObject(0);

					List<SeriePoint> points = new ArrayList<>();

					JsonArray values = resultSerie.getJsonArray("values");

					for (int index = 0; index < values.size(); ++index) {

						String strDateTime = values.getJsonArray(index).getString(0);

						Object value = values.getJsonArray(index).getValue(1);

						ZonedDateTime zdt = ZonedDateTime.parse(strDateTime, DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()));

						points.add(new SeriePoint(value, zdt.toInstant().toEpochMilli()));

					}

					future.complete(new Serie(measurement, tags, points));

				} catch (Exception e) {
					future.fail(e);
					e.printStackTrace();
				}

			} else {
				future.fail(ar.cause());
			}

		});

	}

}
