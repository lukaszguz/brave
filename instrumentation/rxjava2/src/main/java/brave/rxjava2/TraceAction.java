package brave.rxjava2;

import java.util.List;

import brave.Span;
import brave.Tracer;

class TraceAction implements Runnable {

	private static final String RXJAVA_COMPONENT = "rxjava2";
	private final Runnable actual;
	private final Tracer tracer;
	private final Span parent;
	private final List<String> threadsToIgnore;

	TraceAction(Tracer tracer, Runnable actual, List<String> threadsToIgnore) {
		this.tracer = tracer;
		this.threadsToIgnore = threadsToIgnore;
		this.parent = tracer.currentSpan();
		this.actual = actual;
	}

	@Override
	public void run() {
		String threadName = Thread.currentThread().getName();
		if (matchesIgnoredThreadName(threadName)) {
			// don't create a span if the thread name is on a list of threads to ignore
			this.actual.run();
			return;
		}

		Span span = this.parent;
		boolean created = false;
		if (span == null) {
			span = this.tracer.nextSpan().name(RXJAVA_COMPONENT).start();
			span.tag(RxJava2Tags.RXJAVA2_KEY_TAG, Thread.currentThread().getName());
			created = true;
		}
		try (Tracer.SpanInScope ws = this.tracer.withSpanInScope(span)) {
			this.actual.run();
		}
		finally {
			if (created) {
				span.finish();
			}
		}
	}

	private boolean matchesIgnoredThreadName(String threadName) {
		for (String threadToIgnore : this.threadsToIgnore) {
			if (threadName.matches(threadToIgnore)) {
				return true;
			}
		}
		return false;
	}
}
