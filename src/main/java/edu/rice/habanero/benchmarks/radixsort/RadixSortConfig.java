package edu.rice.habanero.benchmarks.radixsort;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class RadixSortConfig {

    protected static int N = 100_000; // data size
    protected static int M = 1 << 30; // max value
    protected static int S = 2_048; // seed for random number generator
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-n":
                    i += 1;
                    N = Integer.parseInt(args[i]);
                    break;
                case "-m":
                    i += 1;
                    M = Integer.parseInt(args[i]);
                    break;
                case "-s":
                    i += 1;
                    S = Integer.parseInt(args[i]);
                    break;
                case "-debug":
                case "-verbose":
                    debug = true;
                    break;
            }
            i += 1;
        }
    }
    
    public static boolean verifyResult(long n) {
    	if (N ==   100 && M ==   256 && S == 74755) { return n ==      13606L; }
    	if (N == 10000 && M == 65536 && S == 74755) { return n ==  329373752L; }
    	if (N == 50000 && M == 65536 && S == 74755) { return n == 1642300184L; }
    	
    	return false;
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num values)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "M (max value)", M);
        System.out.printf(BenchmarkRunner.argOutputFormat, "S (seed)", S);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
