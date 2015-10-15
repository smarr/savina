package som.actors;

import java.util.concurrent.RecursiveAction;

import som.actors.Actor.ActorProcessingThread;
import som.actors.Promise.ResolutionAction;


public abstract class EventualMessage extends RecursiveAction {
	private static final long serialVersionUID = -7994739264831630827L;

	protected EventualMessage() { }

	protected abstract Actor getTarget();
	
	public final void send() {
		getTarget().send(this);
	}

	/**
	 * A message to a known receiver that is to be executed on the actor owning
	 * the receiver.
	 *
	 * ARGUMENTS: are wrapped eagerly on message creation
	 */
	public static abstract class DirectMessage extends EventualMessage {
		private static final long serialVersionUID = 7758943685874210766L;

		private final Actor target;

		public DirectMessage(final Actor target) {
			this.target = target;
		}
 
		@Override
		protected final Actor getTarget() {
			return target;
		}

		@Override
		public String toString() {
			return "DirectMsg(" + (target == null ? "" : target.toString()) + ")";
		}
	}
	
	public static abstract class PromiseMessage extends EventualMessage {
		
	}

//	/**
//	 * A message that was send with <-: to a promise, and will be delivered
//	 * after the promise is resolved.
//	 */
//	public static final class PromiseSendMessage extends PromiseMessage {
//		private static final long serialVersionUID = 2637873418047151001L;
//
//		protected Actor target;
//		protected Actor finalSender;
//
//		protected PromiseSendMessage(final Object[] arguments, 
//				final Actor originalSender, final Resolver resolver,
//				final RootCallTarget onReceive) {
//			super(arguments, originalSender, resolver, onReceive);
//		}
//
//		@Override
//		public void resolve(final Object rcvr, final Actor target,
//				final Actor sendingActor) {
//			determineAndSetTarget(rcvr, target, sendingActor);
//		}
//
//		private void determineAndSetTarget(final Object rcvr,
//				final Actor target, final Actor sendingActor) {
//			args[0] = rcvr;
//			Actor finalTarget = determineTargetAndWrapArguments(args, target,
//					sendingActor, originalSender);
//
//			this.target = finalTarget; // for sends to far references, we need
//										// to adjust the target
//			this.finalSender = sendingActor;
//		}
//
//		@Override
//		protected Actor getTarget() {
//			return target;
//		}
//
//		@Override
//		public String toString() {
//			String t;
//			if (target == null) {
//				t = "null";
//			} else {
//				t = target.toString();
//			}
//			return "PSendMsg(" + Arrays.toString(args) + ", " + t + ", sender: "
//					+ (finalSender == null ? "" : finalSender.toString()) + ")";
//		}
//	}

	/** The callback message to be send after a promise is resolved. */
	public static final class PromiseCallbackMessage extends PromiseMessage {
		private static final long serialVersionUID = 4682874999398510325L;
		private Object result;
		private final ResolutionAction callback;
		private final Actor target;

		public PromiseCallbackMessage(final Actor target,
				final ResolutionAction callback) {
			this.target   = target;
			this.callback = callback;
		}

		@Override
		protected Actor getTarget() {
			return target;
		}
		
		@Override
		protected void executeMessage() {
			callback.action(result);
		}
	}

	@Override
	protected final void compute() {
		Actor target = getTarget();
		setCurrentActor(target);

		try {
			executeMessage();
		} catch (Throwable t) {
			t.printStackTrace();
			throw t;
		}

		setCurrentActor(null);
		target.enqueueNextMessageForProcessing();
	}

	protected void executeMessage() {
		getTarget().receive(this);
	}

	public static Actor getActorCurrentMessageIsExecutionOn() {
		Thread t = Thread.currentThread();
		if (t instanceof ActorProcessingThread) {
			return ((ActorProcessingThread) t).currentlyExecutingActor;
		}
		return mainActor;
	}

	private static void setCurrentActor(final Actor actor) {
		ActorProcessingThread t = (ActorProcessingThread) Thread.currentThread();
		t.currentlyExecutingActor = actor;
	}

	public static void setMainActor(final Actor actor) {
		mainActor = actor;
	}

	private static Actor mainActor;
}
