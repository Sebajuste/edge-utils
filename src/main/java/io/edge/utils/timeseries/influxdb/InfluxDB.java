package io.edge.utils.timeseries.influxdb;

import java.net.URLEncoder;
import java.nio.charset.Charset;
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
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class InfluxDB {

	private final HttpClient client;

	private final InfluxDbOptions options;

	protected InfluxDB(HttpClient client, InfluxDbOptions options) {
		super();
		this.client = client;
		this.options = options;
	}

	public static InfluxDB connect(HttpClient client) {
		return new InfluxDB(client, new InfluxDbOptions());
	}

	public static InfluxDB connect(HttpClient client, InfluxDbOptions options) {
		return new InfluxDB(client, options);
	}

	public static InfluxDB create(Vertx vertx, InfluxDbOptions options) {

		HttpClientOptions httpOptions = new HttpClientOptions();
		httpOptions.setDefaultHost(options.getHost());
		httpOptions.setDefaultPort(options.getPort());

		HttpClient client = vertx.createHttpClient(httpOptions);

		return InfluxDB.connect(client, options);
	}

	private void getRequest(String request, Handler<AsyncResult<Buffer>> handler) {
		final Future<Buffer> future = Future.future();
		future.setHandler(handler);

		HttpClientRequest clientRequest = client.get(request, response -> {

			if (response.statusCode() == HttpResponseStatus.OK.code()) {
				response.bodyHandler(body -> future.complete(body));
			} else if (response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
				future.complete();
			} else {
				response.bodyHandler(body -> {
					if ("Not series found".equalsIgnoreCase(body.toString(Charset.defaultCharset()))) {
						future.complete();
					} else {
						future.fail(body.toString());
					}

				});
			}

		});

		if (options.getUserName() != null && options.getUserName().trim().length() > 0) {
			String auth = Base64.getEncoder().encodeToString((options.getUserName() + ":" + options.getPassword()).getBytes());
			clientRequest.putHeader(HttpHeaders.AUTHORIZATION, "Basic " + auth);
		}

		clientRequest.end();

	}

	private void postRequest(String request, String chunk, Handler<AsyncResult<Void>> handler) {

		final Future<Void> future = Future.future();

		HttpClientRequest clientRequest = client.post(request, response -> {

			if (response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
				future.complete();
			} else {
				response.bodyHandler(body -> future.fail(body.toString()));
			}

		}).putHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");

		if (options.getUserName() != null && options.getUserName().trim().length() > 0) {
			String auth = Base64.getEncoder().encodeToString((options.getUserName() + ":" + options.getPassword()).getBytes());
			clientRequest.putHeader(HttpHeaders.AUTHORIZATION, "Basic " + auth);
		}

		clientRequest.end(chunk);

		future.setHandler(handler);

	}

	public void createDatabase(String dbName, Handler<AsyncResult<Void>> handler) {

		Future<Void> future = Future.future();
		future.setHandler(handler);

		this.getRequest("/query?q=CREATE%20DATABASE%20%22" + dbName + "%22", ar -> {
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

		this.getRequest("/query?q=DELETE%20DATABASE%20%22" + dbName + "%22", ar -> {
			if (ar.succeeded()) {
				future.complete();
			} else {
				future.fail(ar.cause());
			}
		});

	}

	public void write(String dbName, Point point, Handler<AsyncResult<Void>> handler) {
		try {
			this.postRequest("/write?db=" + dbName + "&precision=ms", InfluxDBRequest.writeRequest(point), handler);
		} catch (Exception e) {
			handler.handle(Future.failedFuture(e));
		}
	}

	public void write(BatchPoints batch, Handler<AsyncResult<Void>> handler) {
		try {
			this.postRequest("/write?db=" + batch.getDbName() + "&precision=ms", InfluxDBRequest.writeRequest(batch), handler);
		} catch (Exception e) {
			handler.handle(Future.failedFuture(e));
		}
	}

	public void query(String dbName, String query, Handler<AsyncResult<JsonArray>> resultHandler) {

		Future<JsonArray> future = Future.future();
		future.setHandler(resultHandler);

		try {
			this.getRequest("/query?db=" + dbName + "&q=" + URLEncoder.encode(query, "UTF-8"), ar -> {

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
