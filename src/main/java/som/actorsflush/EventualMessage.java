package som.actorsflush;

import som.actorsflush.Actor.ActorProcessingThread;
import som.actorsflush.Promise.ResolutionAction;


public abstract class EventualMessage {
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

	public final void execute() {
		try {
			executeMessage();
		} catch (Throwable t) {
			t.printStackTrace();
			throw t;
		}
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

	public static void setMainActor(final Actor actor) {
		mainActor = actor;
	}

	private static Actor mainActor;
}
