import java.util.Random;

public class VectorOperations {



    /**
     * This method is used to calculate the dot product between two vectors accepting two parameters
     * @param userVector This is the first parameter to dot method. It is Double[] (the first vector)
     * @param itemVector  This is the second parameter to dot method. It is Double[] (the second vector)
     * @return Double This returns the dot product of the two vectors.
     */
    public static Double dot(Double[] userVector, Double[] itemVector) {
        Double sum = 0.0;
        int latentFeatures = userVector.length;

        for (int k = 0; k < latentFeatures; k++) {
            sum += userVector[k] * itemVector[k];
        }
        return sum;
    }


    /**
     * This method is used to initializes the vector with random number from gaussian distribution with zero mean and 0.1 variance
     * accepting one parameters
     * @param latentFeature This is the first parameter to initializeVector method. It is integer represents the size of the vector)
     * @return Double[] This returns the vector initialized with random number with respect to gaussian distribution.
     */
    public static Double[] initializeVector(int latentFeature){
        Double[] initializedVector =new Double[latentFeature];
        double variance = 0.1;
        Random random = new Random();
        for(int i=0;i<latentFeature;i++){
            //mean = 0 (+0)
            initializedVector[i] = random.nextGaussian()*variance;
        }
        return initializedVector;
    }


    /**
     * This method is used to initializes the vector with zeros.
     * accepting one parameters
     * @param latentFeature This is the first parameter to initializeZeroVector method. It is integer represents the size of the vector)
     * @return Double[] This returns the vector initialized with zeros.
     */
    public static Double[] initializeZeroVector(int latentFeature){
        Double[] initializedVector =new Double[latentFeature];
        for(int i=0;i<latentFeature;i++){
            initializedVector[i] = 0.0;
        }
        return initializedVector;
    }


    /**
     * This method is used to calculates the average vector incrementally(online)
     * accepting three parameters
     * @param lastAvg This is the first parameter to incrementalAvgVector method. It is Double[] (the first vector)
     * @param newVec This is the second parameter to incrementalAvgVector method. It is Double[] represents the second vector)
     * @param counter This is the third parameter to incrementalAvgVector method. It is Double the count of the incremental added vectors
     * @return Double[] This returns the average vector of the two input vectors.
     */
     public static Double[] incrementalAvgVector(Double[] lastAvg ,Double[] newVec, Double counter){

        int vecLen = lastAvg.length;
        Double[] resultVec = new Double[vecLen];
        for(int i=0;i<vecLen;i++){
             resultVec[i] = lastAvg[i] + ((newVec[i]-lastAvg[i])/counter);
         }
         return resultVec;
    }
}
