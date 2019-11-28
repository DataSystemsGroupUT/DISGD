import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SGD {


    userItem userItemVectors = new userItem();

    /**
     * This method is used to update the vectors according to ISGD algorithm
     * @param input_userVector This is the first parameter to update_isgd2 method. It is Double[] (the user vector)
     * @param input_itemVector  This is the second parameter to update_isgd2 method. It is Double[] (the item vector)
     * @param mu  This is the third parameter to update_isgd2 method. It is Double, the step size of ISGD
     * @param lambda  This is the fourth parameter to update_isgd2 method. It is Double, the regularization parameter
     * @return userItem This returns the updated item and user vectors.
     */

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






}


