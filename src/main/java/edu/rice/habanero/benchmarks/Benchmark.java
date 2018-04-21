package edu.rice.habanero.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public abstract class Benchmark {

    final Map<String, List<Double>> customAttrs = new HashMap<>();

    protected void track(final String attrName, final double attrValue) {
        if (!customAttrs.containsKey(attrName)) {
            customAttrs.put(attrName, new ArrayList<>());
        }
        customAttrs.get(attrName).add(attrValue);
    }

    public final String name() {
        return getClass().getSimpleName();
    }

    public final String runtimeInfo() {
        final String javaVersion = System.getProperty("java.version");
        return "Java:" + javaVersion;
    }

    public abstract void initialize(String[] args) throws IOException;

    public abstract void printArgInfo();

    /**
     * Executes benchmark and verifies result. Is expected to return only
     * once both tasks are completed.
     *
     * @return true if verification succeeded, false otherwise
     */
    public boolean runAndVerify() { return false; }

    public abstract void cleanupIteration(boolean lastIteration, double execTimeMillis);

}
