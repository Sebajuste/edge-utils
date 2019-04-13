package io.edge.utils.exchange;

import io.edge.utils.exchange.impl.ExchangeSharedData;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

/**
 * 
 * Voir {@link https://blog.eleven-labs.com/fr/rabbitmq-partie-1-les-bases/}
 * 
 * @author smartinez
 *
 */
public interface Exchange {

	static Exchange exchangeDirect(Vertx vertx, String name) {
		return exchange(vertx, name, new JsonObject());
	}

	static Exchange exchangeFanout(Vertx vertx, String name) {
		return exchange(vertx, name, new JsonObject());
	}

	static Exchange exchangeTopic(Vertx vertx, String name) {
		return exchange(vertx, name, new JsonObject());
	}

	static Exchange exchangeHeaders(Vertx vertx, String name) {
		return exchange(vertx, name, new JsonObject());
	}

	static Exchange exchange(Vertx vertx, String name, JsonObject options) {
		return new ExchangeSharedData(vertx, name, options);
	}

	Exchange start();
	
	Exchange publish(Object message);
	
	Exchange publish(Object message, DeliveryOptions options);

	<T> MessageConsumer<T> consumer(String address, Handler<Message<T>> resultHandler);

	

}
