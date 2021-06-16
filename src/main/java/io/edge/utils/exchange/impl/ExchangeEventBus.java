package io.edge.utils.exchange.impl;

import java.util.HashMap;
import java.util.Map;

import io.edge.utils.exchange.Exchange;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;

public class ExchangeEventBus implements Exchange {

	private final Map<String, Disposable> queueSubscribingMap = new HashMap<>();

	private final Vertx vertx;

	private final String name;

	private ExchangeEventBus(Vertx vertx, String name) {
		super();
		this.vertx = vertx;
		this.name = name;
	}

	@Override
	public Exchange start() {

		this.vertx.eventBus().consumer("vertx.exchange." + name + ".publish", message -> {
			this.vertx.eventBus().publish("vertx.exchange." + name + ".queues", message.body());
		});

		this.vertx.eventBus().consumer("vertx.exchange." + name + ".subscribe", message -> {
			String queueAddress = message.headers().get("queueAddress");
			Disposable disposable = this.connectQueue(queueAddress).subscribe();
			this.queueSubscribingMap.put(queueAddress, disposable);
		});

		this.vertx.eventBus().consumer("vertx.exchange." + name + ".unsubscribe", message -> {

			String queueAddress = message.headers().get("queueAddress");

			if (this.queueSubscribingMap.containsKey(queueAddress)) {
				this.queueSubscribingMap.get(queueAddress).dispose();
			}

		});

		return this;
	}

	/**
	 * Only one call per queue
	 * 
	 * @param address
	 * @return
	 */
	private Flowable<Object> connectQueue(String address) {

		return Flowable.create(emitter -> {

			MessageConsumer<Object> consumer = this.vertx.eventBus().consumer("vertx.exchange." + name + ".queues", message -> {
				this.vertx.eventBus().send(address, message.body());
			});

			emitter.setCancellable(() -> {
				consumer.unregister();
			});

		}, BackpressureStrategy.BUFFER);

	}

	@Override
	public Exchange publish(Object message) {

		this.vertx.eventBus().send("vertx.exchange." + name + ".publish", message);

		return this;
	}
	
	@Override
	public Exchange publish(Object message, DeliveryOptions options)
	{
		this.vertx.eventBus().send("vertx-exchange-" + name + ".publish", message, options);
		return this;
	}

	@Override
	public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> resultHandler) {

		DeliveryOptions subscribeQueueOption = new DeliveryOptions()//
				.addHeader("queueAddress", address);

		this.vertx.eventBus().send("vertx.exchange." + name + ".subscribe", null, subscribeQueueOption);

		return new ExchangeMessageConsumer<T>(this.vertx.eventBus().consumer(address, resultHandler), v -> {
			this.vertx.eventBus().send("vertx.exchange." + name + ".unsubscribe", null, subscribeQueueOption);
		});

	}

}
