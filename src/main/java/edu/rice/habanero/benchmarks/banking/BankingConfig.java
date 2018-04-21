package edu.rice.habanero.benchmarks.banking;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class BankingConfig {

    protected static int A = 1_000; // num accounts
    protected static int N = 50_000; // num transactions
    protected static double INITIAL_BALANCE = Double.MAX_VALUE / (A * N);
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-a":
                    i += 1;
                    A = Integer.parseInt(args[i]);
                    break;
                case "-n":
                    i += 1;
                    N = Integer.parseInt(args[i]);
                    break;
                case "-debug":
                case "-verbose":
                    debug = true;
                    break;
            }
            i += 1;
        }

        INITIAL_BALANCE = ((Double.MAX_VALUE / (A * N)) / 1_000) * 1_000;
    }

    public static boolean verify(final double result) {
      long r = Math.round(result);

      if (N ==   1000) { return r == 516215; }
      if (N ==  10000) { return r == 4975454; }
      if (N ==  50000) { return r == 25057792; }
      if (N == 100000) { return r == 50038643; }
      if (N == 500000) { return r == 250052558; }

      System.out.println("No verification for N: " + N + " r: " + r);

      return false;
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "A (num accounts)", A);
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num transactions)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Initial Balance", INITIAL_BALANCE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static final class StartMessage {
        protected static StartMessage ONLY = new StartMessage();
    }

    protected static final class StopMessage {
        protected static StopMessage ONLY = new StopMessage();
    }

    protected static final class ReplyMessage {
        public final double amount;

        public ReplyMessage(final double amount) {
          this.amount = amount;
        }
    }

    protected static class DebitMessage {
        public final Object sender;
        public final double amount;

        public DebitMessage(final Object sender, final double amount) {
            this.sender = sender;
            this.amount = amount;
        }
    }

    protected static class CreditMessage {
        public final Object sender;
        public final double amount;
        public final Object recipient;

        public CreditMessage(final Object sender, final double amount, final Object recipient) {
            this.sender = sender;
            this.amount = amount;
            this.recipient = recipient;
        }
    }
}
