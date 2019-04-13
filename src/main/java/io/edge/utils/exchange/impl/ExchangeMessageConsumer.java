package io.edge.utils.exchange.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.streams.ReadStream;

public class ExchangeMessageConsumer<T> implements MessageConsumer<T> {

	private final Handler<Void> onUnregisterHandler;

	private final MessageConsumer<T> messageConsumer;

	public ExchangeMessageConsumer(MessageConsumer<T> messageConsumer, Handler<Void> onUnregisterHandler) {
		super();
		this.messageConsumer = messageConsumer;
		this.onUnregisterHandler = onUnregisterHandler;
	}

	@Override
	public MessageConsumer<T> exceptionHandler(Handler<Throwable> handler) {
		return messageConsumer.exceptionHandler(handler);
	}

	@Override
	public MessageConsumer<T> handler(Handler<Message<T>> handler) {
		return messageConsumer.handler(handler);
	}

	@Override
	public MessageConsumer<T> pause() {
		return messageConsumer.pause();
	}

	@Override
	public MessageConsumer<T> resume() {
		return messageConsumer.resume();
	}

	@Override
	public MessageConsumer<T> fetch(long amount) {
		return messageConsumer.fetch(amount);
	}

	@Override
	public MessageConsumer<T> endHandler(Handler<Void> endHandler) {
		return messageConsumer.endHandler(endHandler);
	}

	@Override
	public ReadStream<T> bodyStream() {
		return messageConsumer.bodyStream();
	}

	@Override
	public boolean isRegistered() {
		return messageConsumer.isRegistered();
	}

	@Override
	public String address() {
		return messageConsumer.address();
	}

	@Override
	public MessageConsumer<T> setMaxBufferedMessages(int maxBufferedMessages) {
		return messageConsumer.setMaxBufferedMessages(maxBufferedMessages);
	}

	@Override
	public int getMaxBufferedMessages() {
		return messageConsumer.getMaxBufferedMessages();
	}

	@Override
	public void completionHandler(Handler<AsyncResult<Void>> completionHandler) {
		messageConsumer.completionHandler(completionHandler);

	}

	@Override
	public void unregister() {
		onUnregisterHandler.handle(null);
		messageConsumer.unregister();
	}

	@Override
	public void unregister(Handler<AsyncResult<Void>> completionHandler) {
		messageConsumer.unregister();
	}

}
