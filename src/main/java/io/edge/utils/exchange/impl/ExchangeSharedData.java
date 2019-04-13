package io.edge.utils.exchange.impl;

import java.util.Set;

import io.edge.utils.exchange.Exchange;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Lock;

public class ExchangeSharedData implements Exchange {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeSharedData.class);
	
	private final Vertx vertx;

	private final String name;
	
	private final JsonObject options;

	public ExchangeSharedData(Vertx vertx, String name, JsonObject options) {
		super();
		this.vertx = vertx;
		this.name = name;
		this.options = options;
	}
	
	private void lock(Handler<Future<Void>> resultHandler) {
		
		vertx.sharedData().getLock("vertx.exchange." + name + ".lock", lockResult -> {
			
			if (lockResult.succeeded()) {
				Lock lock = lockResult.result();
				
				Future<Void> futureResult = Future.future(ar -> {
					lock.release();

					if (ar.failed()) {
						LOGGER.error("", ar.cause());
					}

				});
				
				resultHandler.handle(futureResult);
				
				
			} else {
				resultHandler.handle(Future.failedFuture(lockResult.cause()));
			}
			
		});
		
	}
	
	private void connectQueue(Message<Object> message) {
		
		String queueAddress = message.headers().get("queueAddress");
		
		this.lock( future -> {
			
			vertx.sharedData().<String, JsonObject> getAsyncMap("vertx.exchange." + name, ar -> {
				
				if (ar.succeeded()) {
					
					AsyncMap<String, JsonObject> map = ar.result();
					
					map.get(queueAddress, queueConfigResult -> {

						if (queueConfigResult.succeeded()) {

							JsonObject queueConfig = queueConfigResult.result();

							if (queueConfig == null) {
								queueConfig = new JsonObject();
							}

							int subscribeCount = queueConfig.getInteger("subscribeCount", 0);
							queueConfig.put("subscribeCount", subscribeCount + 1);

							map.put(queueAddress, queueConfig, future);

							LOGGER.info("Queue added/updated : " + queueConfig + " at " + queueAddress);
							
							message.reply(null);
							
						} else {
							message.fail(0, queueConfigResult.cause().getMessage());
							future.fail(queueConfigResult.cause());
						}

					});
					
				} else {
					message.fail(0, ar.cause().getMessage());
					future.fail(ar.cause());
				}
				
			});
			
		});
		
	}
	
	private void disconnectQueue(Message<Object> message) {
		
		String queueAddress = message.headers().get("queueAddress");
		
		this.lock( future -> {
			
			vertx.sharedData().<String, JsonObject> getAsyncMap("vertx.exchange." + name, ar -> {
				
				if (ar.succeeded()) {
					
					AsyncMap<String, JsonObject> map = ar.result();
					
					map.get(queueAddress, queueConfigResult -> {

						if (queueConfigResult.succeeded()) {

							JsonObject queueConfig = queueConfigResult.result();

							if (queueConfig != null) {

								int subscribeCount = queueConfig.getInteger("subscribeCount", 0);

								if (subscribeCount == 1) {
									map.remove(queueAddress, removeResult -> {
										if (removeResult.succeeded()) {
											LOGGER.info("Queue removed : " + queueConfig + " at " + queueAddress);
											future.succeeded();
										} else {
											future.fail(removeResult.cause());
										}
									});
								} else {
									queueConfig.put("subscribeCount", subscribeCount - 1);
									map.put(queueAddress, queueConfig, future);
									LOGGER.info("Queue updated : " + queueConfig + " at " + queueAddress);
								}

							} else {
								future.fail(queueConfigResult.cause());
							}

						} else {
							future.fail(queueConfigResult.cause());
						}

					});
					
				} else {
					future.fail(ar.cause());
				}
				
			});
			
		});
		
	}

	private void publishData(Message<Object> message) {

		vertx.sharedData().<String, JsonObject> getAsyncMap("vertx.exchange." + name, ar -> {

			if (ar.succeeded()) {

				AsyncMap<String, JsonObject> map = ar.result();

				map.keys(keysResult -> {

					if (keysResult.succeeded()) {

						Set<String> addressSet = keysResult.result();

						for (String address : addressSet) {

							this.vertx.eventBus().send(address, message.body(), new DeliveryOptions().setHeaders(message.headers()) );

						}

					} else {
						LOGGER.error("Cannot get queue address list", keysResult.cause());
					}

				});

			} else {
				LOGGER.error("Cannot get Exchange queue", ar.cause());
			}

		});
	}

	@Override
	public Exchange start() {

		this.vertx.eventBus().consumer("vertx.exchange." + name + ".queues.connect", this::connectQueue);
		
		this.vertx.eventBus().consumer("vertx.exchange." + name + ".queues.disconnect", this::disconnectQueue);
		
		this.vertx.eventBus().consumer("vertx-exchange-" + name + ".publish", this::publishData);

		return this;
	}

	@Override
	public Exchange publish(Object message) {

		this.vertx.eventBus().send("vertx-exchange-" + name + ".publish", message);

		return this;
	}

	@Override
	public Exchange publish(Object message, DeliveryOptions options)
	{
		this.vertx.eventBus().send("vertx-exchange-" + name + ".publish", message, options);
		return this;
	}
	
	@Override
	public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {

		JsonObject queueConfig = new JsonObject() //
				.put("queueAddress", address);

		final DeliveryOptions options = new DeliveryOptions() //
				.addHeader("queueAddress", address);

		Disposable disposable = Observable.create(emitter -> {
			
			this.vertx.eventBus().send("vertx.exchange." + name + ".queues.connect", queueConfig, options, ar -> {
				if( ar.succeeded()) {
					emitter.onNext(ar.result());
				} else {
					emitter.onError(ar.cause());
				}
			});
			
			emitter.setCancellable(() -> {
				this.vertx.eventBus().send("vertx.exchange." + name + ".queues.disconnect", queueConfig, options);
				
			});
		}).subscribe();

		return new ExchangeMessageConsumer<T>(this.vertx.eventBus().consumer(address, handler), v -> {
			disposable.dispose();
		});

	}
}
