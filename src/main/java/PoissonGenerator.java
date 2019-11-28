public class PoissonGenerator {

    /**
     * This method is used to generate a rondom number from poission distribution accepting one parameters
     * @param lambda This is the first parameter to getPoisson method. It is a double lambda paramter
     * @return int This returns random number with respect to poisson distribution.
     */
    public static int getPoisson(double lambda) {
        double L = Math.exp(-lambda);
        double p = 1.0;
        int k = 0;

        do {
            k++;
            p *= Math.random();
        } while (p > L);

        return k - 1;
    }
}
