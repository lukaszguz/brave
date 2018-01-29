package brave.rxjava2;

import java.util.Collections;
import java.util.List;

import brave.Tracer;
import brave.Tracing;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.SchedulerRunnableIntrospection;

/**
 * Schedule handler is tracing for all schedulers representation.
 */
public class TracingSchedulerHandler implements Function<Runnable, Runnable> {

	private final Tracer tracer;
	private final List<String> threadsToIgnore;
	private final Function<? super Runnable, ? extends Runnable> delegate;

	TracingSchedulerHandler(Tracer tracer, List<String> threadsToIgnore,
			Function<? super Runnable, ? extends Runnable> delegate) {
		this.tracer = tracer;
		this.threadsToIgnore = threadsToIgnore;
		this.delegate = delegate;
	}

	public static TracingSchedulerHandler create(Tracing tracing) {
		return new TracingSchedulerHandler(tracing.tracer(), Collections.emptyList(),
				null);
	}

	public static TracingSchedulerHandler create(Tracing tracing,
			List<String> threadsToSample) {
		return new TracingSchedulerHandler(tracing.tracer(), threadsToSample, null);
	}

	public static TracingSchedulerHandler create(Tracing tracing,
			List<String> threadsToSample,
			Function<? super Runnable, ? extends Runnable> delegate) {
		return new TracingSchedulerHandler(tracing.tracer(), threadsToSample, delegate);
	}

	@Override
	public Runnable apply(Runnable action) throws Exception {
		if (isTraceActionDecoratedByRxWorker(action)) {
			return action;
		}
		Runnable wrappedAction = this.delegate != null ? this.delegate.apply(action)
				: action;
		return new TraceAction(this.tracer, wrappedAction, this.threadsToIgnore);
	}

	private boolean isTraceActionDecoratedByRxWorker(Runnable action) {
		if (action instanceof TraceAction) {
			return true;
		}
		else if (action instanceof SchedulerRunnableIntrospection) {
			SchedulerRunnableIntrospection runnableIntrospection = (SchedulerRunnableIntrospection) action;
			return runnableIntrospection.getWrappedRunnable() instanceof TraceAction;
		}
		return false;
	}
}