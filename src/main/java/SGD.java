import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SGD {


    userItem userItemVectors = new userItem();

    public userItem update_isgd(Double[] input_userVector, Double[] input_itemVector, Double mu, Double lambda) {

        //positive feedback only so the error is subtracted from one(implicit feedback)
        //userItemVectors userItem = new userItemVectors();


        Double error = 1 - (VectorOperations.dot(input_userVector, input_itemVector));

        for (int k = 0; k < input_itemVector.length; k++) {
            double userFeature = input_userVector[k];
            double itemFeature = input_itemVector[k];
            input_userVector[k] += mu * (error * itemFeature - lambda * userFeature);
            input_itemVector[k] += mu * (error * input_userVector[k] - lambda * itemFeature);
        }

        userItemVectors.userVector = input_userVector;
        userItemVectors.itemVector = input_itemVector;

        return userItemVectors;
    }

    public userItem update_isgd2(Double[] input_userVector, Double[] input_itemVector, Double mu, Double lambda) {

        //positive feedback only so the error is subtracted from one(implicit feedback)
        //userItemVectors userItem = new userItemVectors();
        Double[] iVec = input_itemVector.clone();
        Double[] uVec = input_userVector.clone();


        Double error = 1 - (VectorOperations.dot(uVec, iVec));

        for (int k = 0; k < input_itemVector.length; k++) {
            double userFeature = uVec[k];
            double itemFeature = iVec[k];
            uVec[k] += mu * (error * itemFeature - lambda * userFeature);
            iVec[k] += mu * (error * uVec[k] - lambda * itemFeature);
        }

        userItemVectors.userVector = uVec;
        userItemVectors.itemVector = iVec;

        return userItemVectors;
    }


    public userItem update_sgd(Double[] input_userVector, Double[] input_itemVector, Double rate, Double mu, Double lambda, Integer iterations) {
        //Explicit feedback
        //userItemVectors userItem = new userItemVectors();
        for (int n = 0; n < iterations; n++) {
            Double error = rate - (VectorOperations.dot(input_userVector, input_itemVector));
            for (int k = 0; k < input_userVector.length; k++) {
                double userFeature = input_userVector[k];
                double itemFeature = input_itemVector[k];
                input_userVector[k] += mu * (error * itemFeature - lambda * userFeature);
                input_itemVector[k] += mu * (error * userFeature - lambda * itemFeature);
            }
        }

        userItemVectors.userVector = input_userVector;
        userItemVectors.itemVector = input_itemVector;

        return userItemVectors;
    }

    public ArrayList<String> recommend_isgd(Iterable<Map.Entry<String, Double[]>> itemsVectors, Double[] userVector, ArrayList<String> ratedItems, Integer N) {

        Map<String, Double> scores = new HashMap<>();
        ArrayList<String> recommendedItems = new ArrayList<>();

        ArrayList<String> testScores = new ArrayList<>();

        for (Map.Entry<String, Double[]> item : itemsVectors) {
            //we should not recommend to the user movie he rated before
            if (ratedItems.contains(item.getKey())) {
                continue;
            }

            Double score = Math.abs(1 - VectorOperations.dot(userVector, item.getValue()));
            scores.put(item.getKey(), score);
            //testScores.add(item.getKey());
            //testScores.add(String.valueOf(score));
        }

        //sorting scores to get the taste of the user
        //scores.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        scores.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .limit(N)
                .forEach(itemRate -> recommendedItems.add(itemRate.getKey()));

        return recommendedItems;

    }

    public String recItemChecker(Integer index, ArrayList<String> recommenderItems) {
        if (index >= recommenderItems.size()) {
            return "Null";
        } else {
            return recommenderItems.get(index);
        }
    }
}


