package som;

/**
 * A small portable pseudo-random-number-generator.
 * It is modeled after Java's and SOM's Random class, but is kept minimal, 
 * and to be used only in a thread-local way. 
 */
public final class Random {
	
	private int seed;
	private boolean gotNextGaussian;
	private double nextNextGaussian;
	
	public Random(int seed) {
		this.seed = seed;
		gotNextGaussian  = false;
		nextNextGaussian = 0.0;
	}
	
	public Random() {
		this(74755);
	}
	
	public int next() {
		seed = ((seed * 1309) + 13849) & 65535;
		return seed;
	}

	/** Returns an integer within the range of [0, bound). */
	public int next(int bound) {
		return next() % bound;
	}
			    
    /** Returns a double uniformly distributed in the range of [0.0, 1.0). */
    public double nextDouble() {
    	return next() / 65536.0;
    }
    
    public boolean nextBoolean() {
      return next() < 32768;
    }
    
    /** Returns a double normally distributed with mean 0.0 
       and standard deviation of 1.0. */
    public double nextGaussian() {
      if (gotNextGaussian) {
        gotNextGaussian = false;
        return nextNextGaussian;
      }
      
      double v1 = (2.0 * nextDouble()) - 1.0;
      double v2 = (2.0 * nextDouble()) - 1.0;
      double s  = (v1 * v1) + (v2 * v2);
      
      while ((s >= 1.0) || (s == 0.0)) {
        v1 = (2.0 * nextDouble()) - 1.0;
        v2 = (2.0 * nextDouble()) - 1.0;
        s  = (v1 * v1) + (v2 * v2);
      }
      
      double multiplier = Math.sqrt(-2.0 * Math.log(s) / s);
      nextNextGaussian = v2 * multiplier;
      gotNextGaussian = true;
      return v1 * multiplier;
    }
}
