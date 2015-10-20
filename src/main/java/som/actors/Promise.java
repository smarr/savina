package som.actors;

import java.util.ArrayList;

import som.actors.EventualMessage.PromiseCallbackMessage;
import som.actors.EventualMessage.PromiseMessage;

public class Promise {
	
	@FunctionalInterface
	public interface ResolutionAction {
		public abstract void action(Object result);
	}
	
	private enum Resolution {
		UNRESOLVED, ERRORNOUS, SUCCESSFUL, CHAINED
	}

	// THREAD-SAFETY: these fields are subject to race conditions and should
	// only
	// be accessed when under the SPromise(this) lock
	// currently, we minimize locking by first setting the result
	// value and resolved flag, and then release the lock again
	// this makes sure that the promise owner directly schedules
	// call backs, and does not block the resolver to schedule
	// call backs here either. After resolving the future,
	// whenResolved and whenBroken should only be accessed by the
	// resolver actor
	protected PromiseMessage whenResolved;
	protected ArrayList<PromiseMessage> whenResolvedExt;
	protected ArrayList<PromiseMessage> onError;

	protected Promise chainedPromise;
	protected ArrayList<Promise> chainedPromiseExt;

	protected Object value;
	protected Resolution resolutionState;

	// the owner of this promise, on which all call backs are scheduled
	protected final Actor owner;

	public Promise(final Actor owner) {
	    this.owner = owner;
	    resolutionState = Resolution.UNRESOLVED;
	}

	@Override
	public String toString() {
		String r = "Promise[" + owner.toString();
		r += ", " + resolutionState.name();
		return r + (value == null ? "" : ", " + value.toString()) + "]";
	}

	public final Actor getOwner() {
		return owner;
	}
	
	/** This should only be used on the main thread. */
	public synchronized Object await() {
		try {
			this.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return value;
	}

	public final Promise onError(final ResolutionAction callback, final Actor current) {
		Promise  promise  = new Promise(current);
//		Resolver resolver = new Resolver(promise);

		PromiseCallbackMessage msg = new PromiseCallbackMessage(owner, callback);
		registerOnError(msg);
		return promise;
	}

	public synchronized void whenResolved(ResolutionAction callback) {
		registerWhenResolvedUnsynced(new PromiseCallbackMessage(owner, callback));
	}
	
	final void registerWhenResolvedUnsynced(final PromiseMessage msg) {
		if (whenResolved == null) {
			whenResolved = msg;
		} else {
			registerMoreWhenResolved(msg);
		}
	}

	private void registerMoreWhenResolved(final PromiseMessage msg) {
		if (whenResolvedExt == null) {
			whenResolvedExt = new ArrayList<>(2);
		}
		whenResolvedExt.add(msg);
	}

	public synchronized void registerOnError(final PromiseMessage msg) {
		if (resolutionState == Resolution.ERRORNOUS) {
			scheduleCallbacksOnResolution(value, msg);
		} else {
			if (resolutionState == Resolution.SUCCESSFUL) {
				// short cut on resolved, this promise will never error, so,
				// just return promise, don't use isSomehowResolved(), because
				// the other
				// case are not correct
				return;
			}
			if (onError == null) {
				onError = new ArrayList<>(1);
			}
			onError.add(msg);
		}
	}

	protected final void scheduleCallbacksOnResolution(final Object result,
			final PromiseMessage msg) {
		// when a promise is resolved, we need to schedule all the
		// #whenResolved:/#onError:/... callbacks msgs as well as all eventual
		// send
		// msgs to the promise

		assert owner != null;
//		msg.resolve(result, owner, current);
		Actor target = msg.getTarget();
		if (target != null) {
			target.send(msg);
		} else {
			msg.executeMessage();
		}
	}

	public final synchronized void addChainedPromise(final Promise promise) {
		assert promise != null;
		promise.resolutionState = Resolution.CHAINED;

		if (chainedPromise == null) {
			chainedPromise = promise;
		} else {
			addMoreChainedPromises(promise);
		}
	}

	private void addMoreChainedPromises(final Promise promise) {
		if (chainedPromiseExt == null) {
			chainedPromiseExt = new ArrayList<>(2);
		}
		chainedPromiseExt.add(promise);
	}

	public final synchronized boolean isSomehowResolved() {
		return resolutionState != Resolution.UNRESOLVED;
	}

	/** Internal Helper, only to be used properly synchronized. */
	final boolean isResolvedUnsync() {
		return resolutionState == Resolution.SUCCESSFUL;
	}

	/** Internal Helper, only to be used properly synchronized. */
	final boolean isErroredUnsync() {
		return resolutionState == Resolution.ERRORNOUS;
	}

	/** Internal Helper, only to be used properly synchronized. */
	final Object getValueUnsync() {
		return value;
	}

	/** REM: this method needs to be used with self synchronized. */
	final void copyValueToRemotePromise(final Promise remote) {
		remote.value = value;
		remote.resolutionState = resolutionState;
	}

	public static class Resolver {
		protected final Promise promise;

		public Resolver(final Promise promise) {
			this.promise = promise;
		}

		public final Promise getPromise() {
			return promise;
		}

		@Override
		public String toString() {
			return "Resolver[" + promise.toString() + "]";
		}

		public final boolean assertNotResolved() {
			assert !promise
					.isSomehowResolved() : "Not sure yet what to do with re-resolving of promises? just ignore it? Error?";
			assert promise.value == null : "If it isn't resolved yet, it shouldn't have a value";
			return true;
		}
		
		public void resolve(Object result) {
			resolveAndTriggerListeners(result, promise);
		}

		// TODO: solve the TODO and then remove the TruffleBoundary, this might
		// even need to go into a node
		protected static void resolveChainedPromises(final Promise promise,
				final Object result) {
			// TODO: we should change the implementation of chained promises to
			// always move all the handlers to the other promise, then we
			// don't need to worry about traversing the chain, which can
			// lead to a stack overflow.
			// TODO: restore 10000 as parameter in
			// testAsyncDeeplyChainedResolution
			if (promise.chainedPromise != null) {
				resolveAndTriggerListeners(result, promise.chainedPromise);
				resolveMoreChainedPromises(promise, result);
			}
		}

		private static void resolveMoreChainedPromises(final Promise promise,
				final Object result) {
			if (promise.chainedPromiseExt != null) {

				for (Promise p : promise.chainedPromiseExt) {
					resolveAndTriggerListeners(result, p);
				}
			}
		}

		protected static void resolveAndTriggerListeners(final Object result,
				final Promise p) {
			assert !(result instanceof Promise);

			synchronized (p) {
				p.value = result;
				p.resolutionState = Resolution.SUCCESSFUL;
				scheduleAll(p, result);
				resolveChainedPromises(p, result);
			}
		}

		protected static void scheduleAll(final Promise promise,
				final Object result) {
			if (promise.whenResolved != null) {
				promise.scheduleCallbacksOnResolution(result, promise.whenResolved);
				scheduleExtensions(promise, result);
			}
		}

		private static void scheduleExtensions(final Promise promise,
				final Object result) {
			if (promise.whenResolvedExt != null) {
				for (int i = 0; i < promise.whenResolvedExt.size(); i++) {
					PromiseMessage callbackOrMsg = promise.whenResolvedExt.get(i);
					promise.scheduleCallbacksOnResolution(result, callbackOrMsg);
				}
			}
		}
	}
}
