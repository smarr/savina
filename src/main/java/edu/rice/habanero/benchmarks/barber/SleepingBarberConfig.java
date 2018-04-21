package edu.rice.habanero.benchmarks.barber;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import som.Random;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class SleepingBarberConfig {

    protected static int N = 5_000; // num haircuts
    protected static int W = 1_000; // waiting room size
    protected static int APR = 1_000; // average production rate
    protected static int AHR = 1_000; // avergae haircut rate
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            if ("-n".equals(loopOptionKey)) {
                i += 1;
                N = Integer.parseInt(args[i]);
            } else if ("-w".equals(loopOptionKey)) {
                i += 1;
                W = Integer.parseInt(args[i]);
            } else if ("-pr".equals(loopOptionKey)) {
                i += 1;
                APR = Integer.parseInt(args[i]);
            } else if ("-hr".equals(loopOptionKey)) {
                i += 1;
                AHR = Integer.parseInt(args[i]);
            } else if ("-debug".equals(loopOptionKey) || "-verbose".equals(loopOptionKey)) {
                debug = true;
            }
            i += 1;
        }
    }

    public static boolean verify(final int result) {
      if (N ==  800 && AHR ==  200) { return result == 11357; }
      if (N == 1000 && AHR ==  500) { return result ==  5029; }
      if (N == 2500 && AHR ==  500) { return result == 16033; }
      if (N == 2500 && AHR == 1000) { return result == 32109; }
      if (N == 5000 && AHR == 1000) { return result == 32109; }

      System.out.println("no verification result for AHR: " + AHR + " result is: " + result);
      return false;
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num haircuts)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "W (waiting room size)", W);
        System.out.printf(BenchmarkRunner.argOutputFormat, "APR (production rate)", APR);
        System.out.printf(BenchmarkRunner.argOutputFormat, "AHR (haircut rate)", AHR);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static int busyWait(final Random random, final int limit) {
        int test = 0;

        for (int k = 0; k < limit; k++) {
            random.next();
            test++;
        }

        return test;
    }

    protected static class Full {
        public static Full ONLY = new Full();
    }

    protected static class Wait {
        public static Wait ONLY = new Wait();
    }

    protected static class Next {
        public static Next ONLY = new Next();
    }

    protected static class Start {
        public static Start ONLY = new Start();
    }

    protected static class Done {
        public static Done ONLY = new Done();
    }

    protected static class Exit {
        public static Exit ONLY = new Exit();
    }

}
