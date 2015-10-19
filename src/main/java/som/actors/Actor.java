package som.actors;

import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;

import som.actors.Actor.ActorProcessingThread;

import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;


public abstract class Actor {
	/** buffer for incoming messages */ 
	private ArrayList<EventualMessage> mailbox = new ArrayList<>();
	private ExecAllMessages executor;
	
	/** flag to indicate whether there is currently a F/J task executing */
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
		mailbox.add(msg);
		logMessageAddedToMailbox(msg);
		
		if (!isExecuting) {
			isExecuting = true;
			executeOnPool();
		}
	}

	private static class ExecAllMessages implements Runnable {
		
		private final Actor actor;
		private ArrayList<EventualMessage> current;
		private ArrayList<EventualMessage> emptyUnused;
		
		public ExecAllMessages(Actor actor) {
			this.actor = actor;
		}

		@Override
		public void run() {
			ActorProcessingThread t = (ActorProcessingThread) Thread.currentThread();
			t.currentlyExecutingActor = actor;
			
			// grab current mailbox from actor
			if (emptyUnused == null) {
				emptyUnused = new ArrayList<>();
			}
			
			while (getCurrentMessagesOrCompleteExecution()) {
				processCurrentMessages();
			}
			
			t.currentlyExecutingActor = null;
		}
		
		private void processCurrentMessages() {
			int size = current.size();
			for (int i = 0; i < size; i++) {
				EventualMessage msg = current.get(i);
				actor.logMessageBeingExecuted(msg);
				msg.execute();
			}
			current.clear();
			emptyUnused = current;
		}

		private boolean getCurrentMessagesOrCompleteExecution() {
			synchronized (actor) {
				current = actor.mailbox;
				if (current.isEmpty()) {
					// complete execution after all messages are processed
					actor.isExecuting = false;
					return false;
				}
				actor.mailbox = emptyUnused;
			}
			return true;
		}
	}
	
	/**
	 * Schedule the actor's executor on the fork/join pool.
	 */
	private void executeOnPool() {
		if (executor == null) {
			executor = new ExecAllMessages(this);
		}
		actorPool.execute(executor);
	}

	/**
	 * @return true, if there are no scheduled submissions, 
	 *         and no active threads in the pool, false otherwise.
	 *         This is only best effort, it does not look at the actor's
	 *         message queues.
	 */
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
	
//	/**
//	 * This method is only to be called from the EventualMessage task, and the
//	 * main Actor in Bootstrap.executeApplication().
//	 */
//	public final synchronized void enqueueNextMessageForProcessing() {
//		try {
//			EventualMessage nextTask = mailbox.remove();
//			assert isExecuting;
//			executeOnPool(nextTask);
//			logMessageBeingExecuted(nextTask);
//			return;
//		} catch (NoSuchElementException e) {
//			logNoTaskForActor();
//			isExecuting = false;
//		}
//	}
}
