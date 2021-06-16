package io.edge.utils.exchange;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ExchangeTest {

	@Rule
	public RunTestOnContext rule = new RunTestOnContext();

	public void test() {

		TestSuite suite = TestSuite.create("test_suite_1");

		suite.test("testPublish", this::testPublish);

		suite.run();
	}
	
	@Test
	public void testUnsubscribe(TestContext context) {
		
		Vertx vertx = rule.vertx();
		
		Async async = context.async();

		Exchange exchange = Exchange.exchangeFanout(vertx, "exch1");
		
		exchange.consumer("queue1", ar -> {
			
		}).unregister();
		
		vertx.setTimer(100L, id -> {
			async.complete();
		});
		
	}

	// @Test
	public void testPublish(TestContext context) {
		
		Vertx vertx = rule.vertx();
		
		Async async = context.async();

		Exchange exchange = Exchange.exchangeFanout(vertx, "exch1").start();
		
		final AtomicInteger counter1 = new AtomicInteger();
		
		exchange.consumer("queue1", ar -> {
			counter1.incrementAndGet();
		});
		
		exchange.consumer("queue1", ar -> {
			counter1.incrementAndGet();
		});
		
		final AtomicInteger counter2 = new AtomicInteger();
		exchange.consumer("queue2", ar -> {
			counter2.incrementAndGet();
		});
		
		vertx.setTimer(100L, id1 -> {
			
			exchange.publish("Hello World");
			
			vertx.setTimer(100L, id2 -> {
			
				context.assertEquals(1, counter1.get());
				context.assertEquals(1, counter2.get());
			
				async.complete();
			});
		});

		
	}

}
