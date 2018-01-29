package brave.rxjava2;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.BDDAssertions.then;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Function;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;

public class TracingSchedulerHandlerTest {

	private List<String> threadsToIgnore = new ArrayList<>();
	private ArrayListSpanReporter reporter = new ArrayListSpanReporter();
	private Tracing tracing = Tracing.newBuilder()
			.currentTraceContext(CurrentTraceContext.Default.create())
			.spanReporter(this.reporter).build();
	private Tracer tracer = this.tracing.tracer();

	@After
	public void clean() {
		this.tracing.close();
		this.reporter.clear();
	}

	private static StringBuilder caller;

	@Before
	@After
	public void setup() {
		RxJavaPlugins.reset();
		caller = new StringBuilder();
	}

	@Test
	public void should_wrap_delegates_action_in_wrapped_action_when_delegate_is_present_on_schedule() {
		Function<? super Runnable, ? extends Runnable> delegate = (runnable) -> () -> {
			caller = new StringBuilder("called_from_schedulers_hook");
		};

		TracingSchedulerHandler tracingSchedulerHandler = TracingSchedulerHandler
				.create(this.tracing, threadsToIgnore, delegate);
		RxJavaPlugins.setScheduleHandler(tracingSchedulerHandler);

		Runnable action = RxJavaPlugins
				.onSchedule(() -> caller = new StringBuilder("hello"));
		action.run();
		then(action).isInstanceOf(TraceAction.class);
		then(caller.toString()).isEqualTo("called_from_schedulers_hook");
	}

	@Test
	public void should_not_create_a_span_when_current_thread_should_be_ignored()
			throws ExecutionException, InterruptedException {
		ExecutorService executorService = Executors
				.newSingleThreadExecutor(customThreadFactory("MyCustomThread10"));
		String threadNameToIgnore = "^MyCustomThread.*$";
		Function<? super Runnable, ? extends Runnable> delegate = (runnable) -> {
			caller = new StringBuilder("called_from_schedulers_hook");
			return runnable;
		};

		TracingSchedulerHandler tracingSchedulerHandler = TracingSchedulerHandler.create(
				this.tracing, Collections.singletonList(threadNameToIgnore), delegate);
		RxJavaPlugins.setScheduleHandler(tracingSchedulerHandler);

		Future<Void> hello = executorService.submit(() -> {
			Runnable action = RxJavaPlugins
					.onSchedule(() -> caller = new StringBuilder("hello"));
			action.run();
			return null;
		});

		hello.get();

		then(this.reporter.getSpans()).isEmpty();
		then(this.tracer.currentSpan()).isNull();
		then(caller.toString()).isEqualTo("hello");
	}

	@Test
	public void should_create_new_span_when_rx_java_action_is_executed_and_there_was_no_span() {
		TracingSchedulerHandler tracingSchedulerHandler = TracingSchedulerHandler
				.create(this.tracing, Collections.singletonList("thread-name"));
		RxJavaPlugins.setScheduleHandler(tracingSchedulerHandler);

		Observable.fromCallable(
				() -> (Runnable) () -> caller = new StringBuilder("actual_action"))
				.subscribeOn(Schedulers.io()).blockingSubscribe(Runnable::run);

		then(caller.toString()).isEqualTo("actual_action");
		then(this.tracer.currentSpan()).isNull();
		await().atMost(5, SECONDS)
				.untilAsserted(() -> then(this.reporter.getSpans()).hasSize(1));
		zipkin2.Span span = this.reporter.getSpans().get(0);
		then(span.name()).isEqualTo("rxjava2");
	}

	@Test
	public void should_create_new_span_when_rx_java_action_is_executed_and_there_was_no_span_and_on_thread_pool_changed() {
		Scheduler scheduler = Schedulers
				.from(Executors.newSingleThreadExecutor(customThreadFactory("myPool")));
		Scheduler scheduler2 = Schedulers
				.from(Executors.newSingleThreadExecutor(customThreadFactory("myPool2")));

		TracingSchedulerHandler tracingSchedulerHandler = TracingSchedulerHandler
				.create(this.tracing);
		RxJavaPlugins.setScheduleHandler(tracingSchedulerHandler);

		Observable.fromCallable(
				() -> (Runnable) () -> caller = new StringBuilder("actual_action"))
				.observeOn(scheduler).observeOn(scheduler2).subscribeOn(Schedulers.io())
				.blockingSubscribe(Runnable::run);

		then(caller.toString()).isEqualTo("actual_action");
		then(this.tracer.currentSpan()).isNull();
		await().atMost(5, SECONDS)
				.untilAsserted(() -> then(this.reporter.getSpans()).hasSize(1));
		zipkin2.Span span = this.reporter.getSpans().get(0);
		then(span.name()).isEqualTo("rxjava2");
	}

	@Test
	public void should_continue_current_span_when_rx_java_action_is_executed() {
		Span spanInCurrentThread = this.tracer.nextSpan().name("current_span");

		try (Tracer.SpanInScope ws = this.tracer.withSpanInScope(spanInCurrentThread)) {
			Observable.fromCallable(
					() -> (Runnable) () -> caller = new StringBuilder("actual_action"))
					.subscribeOn(Schedulers.newThread()).blockingSubscribe(Runnable::run);
		}
		finally {
			spanInCurrentThread.finish();
		}
		then(caller.toString()).isEqualTo("actual_action");
		then(this.tracer.currentSpan()).isNull();
		// making sure here that no new spans were created or reported as closed
		then(this.reporter.getSpans()).hasSize(1);
		zipkin2.Span span = this.reporter.getSpans().get(0);
		then(span.name()).isEqualTo("current_span");
	}

	private ThreadFactory customThreadFactory(String threadName) {
		return runnable -> {
			Thread thread = new Thread(runnable);
			thread.setName(threadName);
			return thread;
		};
	}
}