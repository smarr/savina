package som.actors;

import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;

public abstract class Actor {
	private final ArrayDeque<EventualMessage> mailbox = new ArrayDeque<>();
	private boolean isExecuting;

	protected Actor() {
		isExecuting = false;
	}
	
	public abstract void receive(EventualMessage msg);

	protected void logMessageAddedToMailbox(final EventualMessage msg) { }
	protected void logMessageBeingExecuted(final EventualMessage msg) {	}
	protected void logNoTaskForActor() { }

	public final synchronized void send(final EventualMessage msg) {
		assert msg.getTarget() == this;
		if (isExecuting) {
			mailbox.addLast(msg);
			logMessageAddedToMailbox(msg);
		} else {
			executeOnPool(msg);
			logMessageBeingExecuted(msg);
			isExecuting = true;
		}
	}

	/**
	 * Execute the message on the actor pool.
	 */
	public static void executeOnPool(final EventualMessage msg) {
		actorPool.execute(msg);
	}

	public static boolean isPoolIdle() {
		return !actorPool.hasQueuedSubmissions() && actorPool.getActiveThreadCount() == 0;
	}

	private static final class ActorProcessingThreadFactor implements ForkJoinWorkerThreadFactory {
		@Override
		public ForkJoinWorkerThread newThread(final ForkJoinPool pool) {
			return new ActorProcessingThread(pool);
		}
	}

	public static final class ActorProcessingThread extends ForkJoinWorkerThread {
		protected Actor currentlyExecutingActor;

		protected ActorProcessingThread(final ForkJoinPool pool) {
			super(pool);
		}
	}
	
	private static int getNumWorkers(String propertyName) {
		String prop = System.getProperty(propertyName);
		if (prop == null) {
			return Runtime.getRuntime().availableProcessors();
		} else {
			return Integer.valueOf(prop);
		}
	}
	
	private static final ForkJoinPool actorPool = new ForkJoinPool(
			getNumWorkers("actors.corePoolSize"),
			new ActorProcessingThreadFactor(), null, true);

	public static final void awaitQuiescence() {
		actorPool.awaitQuiescence(10000, TimeUnit.DAYS);
	} 
	
	/**
	 * This method is only to be called from the EventualMessage task, and the
	 * main Actor in Bootstrap.executeApplication().
	 */
	public final synchronized void enqueueNextMessageForProcessing() {
		try {
			EventualMessage nextTask = mailbox.remove();
			assert isExecuting;
			executeOnPool(nextTask);
			logMessageBeingExecuted(nextTask);
			return;
		} catch (NoSuchElementException e) {
			logNoTaskForActor();
			isExecuting = false;
		}
	}
}
