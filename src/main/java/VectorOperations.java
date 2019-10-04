import java.util.Random;

public class VectorOperations {


    //The dot method applies dot product between two vectors
    public static Double dot(Double[] userVector, Double[] itemVector) {
        Double sum = 0.0;
        int latentFeatures = userVector.length;

        for (int k = 0; k < latentFeatures; k++) {
            sum += userVector[k] * itemVector[k];
        }
        return sum;
    }


    //initializeVector method initializes the vector with random number from gaussian with zero mean and 0.1 variance
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


    //initializeZeroVector initializes the vector with zeros
    public static Double[] initializeZeroVector(int latentFeature){
        Double[] initializedVector =new Double[latentFeature];
        for(int i=0;i<latentFeature;i++){
            initializedVector[i] = 0.0;
        }
        return initializedVector;
    }


    //incrementalAvgVector calculates the average vector incrementally(online)
     public static Double[] incrementalAvgVector(Double[] lastAvg ,Double[] newVec, Double counter){

        int vecLen = lastAvg.length;
        Double[] resultVec = new Double[vecLen];
        for(int i=0;i<vecLen;i++){
             resultVec[i] = lastAvg[i] + ((newVec[i]-lastAvg[i])/counter);
         }
         return resultVec;
    }
}
