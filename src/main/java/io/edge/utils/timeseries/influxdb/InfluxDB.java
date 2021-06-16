package io.edge.utils.timeseries.influxdb;

import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
import io.vertx.core.Promise;
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

	private Future<Buffer> getRequest(String requestURI, JsonObject queryParams) {

		final Promise<Buffer> promise = Promise.promise();
		

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
					promise.complete(body);
				} else if (response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
					promise.complete();
				} else {

					if ("Not series found".equalsIgnoreCase(body.toString())) {
						promise.complete();
					} else {
						promise.fail(body.toString());
					}
				}

			} else {
				promise.fail(ar.cause());
			}
		});

		return promise.future();
	}

	private Future<Void> postRequest(String requestURI, JsonObject queryParams, Buffer chunk) {

		final Promise<Void> promise = Promise.promise();

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
					promise.complete();
				} else {
					promise.fail(response.body().toString());
				}

			} else {
				promise.fail(ar.cause());
			}

		});

		return promise.future();
	}

	public Future<Void> createDatabase(String dbName) {

		JsonObject params = new JsonObject()//
				.put("q", "CREATE DATABASE \"" + dbName + "\"");

		return this.getRequest("/query", params).map(buffer -> null);

	}

	public Future<Void> deleteDatabase(String dbName, Handler<AsyncResult<Void>> handler) {
		
		JsonObject params = new JsonObject()//
				.put("q", "DELETE DATABASE \"" + dbName + "\"");

		return this.getRequest("/query", params).map(buffer -> null);

	}

	public Future<Void> write(String dbName, Point point) {
		try {

			JsonObject params = new JsonObject()//
					.put("db", dbName)//
					.put("precision", "ms");

			return this.postRequest("/write", params, InfluxDBRequest.writeRequest(point)).map(buffer -> null);
		} catch (Exception e) {
			return Future.failedFuture(e);
		}
	}

	public Future<Void> write(BatchPoints batch) {
		try {

			JsonObject params = new JsonObject()//
					.put("db", batch.getDbName())//
					.put("precision", "ms");

			return this.postRequest("/write", params, InfluxDBRequest.writeRequest(batch)).map(buffer -> null);

		} catch (Exception e) {
			return Future.failedFuture(e);
		}
	}

	public Future<JsonArray> query(String dbName, String query) {
		
		try {

			JsonObject params = new JsonObject()//
					.put("db", dbName)//
					.put("q", URLEncoder.encode(query, "UTF-8"));

			return this.getRequest("/query", params).map(buffer -> {
				
				JsonObject body = new JsonObject(buffer);
				JsonArray resultSeries = body.getJsonArray("results").getJsonObject(0).getJsonArray("series");
				
				if (resultSeries == null) {
					throw new RuntimeException("Not series found");
				}
				
				return resultSeries;

			});
		} catch (Exception e) {
			return Future.failedFuture(e);
		}

	}

	public Future<Serie> querySerie(String dbName, String measurement, LocalDateTime startDateTime, JsonObject tags, String timeGroup) {
		return this.querySerie(dbName, measurement, startDateTime, null, tags, timeGroup);
	}

	public Future<Serie> querySerie(String dbName, String measurement, LocalDateTime startDateTime, LocalDateTime endDateTime, JsonObject tags, String timeGroup) {
		return this.querySerie(dbName, measurement, startDateTime, endDateTime, ZoneId.systemDefault(), tags, timeGroup);
	}

	public Future<Serie> querySerie(String dbName, String measurement, LocalDateTime startDateTime, LocalDateTime endDateTime, ZoneId zone, JsonObject tags, String timeGroup) {

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

		
		return this.query(dbName, query)//
		.map(series -> {
			JsonObject resultSerie = series.getJsonObject(0);

			List<SeriePoint> points = new ArrayList<>();

			JsonArray values = resultSerie.getJsonArray("values");

			for (int index = 0; index < values.size(); ++index) {

				String strDateTime = values.getJsonArray(index).getString(0);

				Object value = values.getJsonArray(index).getValue(1);

				ZonedDateTime zdt = ZonedDateTime.parse(strDateTime, DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()));

				points.add(new SeriePoint(value, zdt.toInstant().toEpochMilli()));

			}
			
			return new Serie(measurement, tags, points);
		});
		
	}

}
