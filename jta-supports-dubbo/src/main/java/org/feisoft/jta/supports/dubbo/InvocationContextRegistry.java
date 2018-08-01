
package org.feisoft.jta.supports.dubbo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class InvocationContextRegistry {
	private static final InvocationContextRegistry instance = new InvocationContextRegistry();

	private final Map<Thread, InvocationContext> contexts = new ConcurrentHashMap<Thread, InvocationContext>();

	private InvocationContextRegistry() {
		if (instance != null) {
			throw new IllegalStateException();
		}
	}

	public static InvocationContextRegistry getInstance() {
		return instance;
	}

	public void associateInvocationContext(InvocationContext context) {
		this.contexts.put(Thread.currentThread(), context);
	}

	public InvocationContext desociateInvocationContext() {
		return this.contexts.remove(Thread.currentThread());
	}

	public InvocationContext getInvocationContext() {
		return this.contexts.get(Thread.currentThread());
	}

}
