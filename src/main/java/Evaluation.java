import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;

public class Evaluation {

    //*** Evaluation class includes the evaluation methods for online recommender systems**//



    /**
     * This method is used to calculate the precision considering prequential evaluation accepting two parameters
     * precisionOnline calculates online precision = |E ∩ L| / k.
     * @param consumedItem This is the first parameter to precisionOnline method. It is a string item in the current interaction
     * @param recommendedList  This is the second parameter to precisionOnline method. It is an Arraylist of the recommended items
     * @return Float This returns online precision for the current interaction.
     */
    Float precisionOnline(String consumedItem, ArrayList<String> recommendedList){
        Integer k = recommendedList.size();
        if (recommendedList.contains(consumedItem)){
            return (float)1/k;
        }
        else {
            return 0f;
        }
    }


    /**
     * This method is used to calculate the recall considering prequential evaluation accepting two parameters
     * recallOnline calculates online Recall = |E ∩ L| / |E|.
     * @param consumedItem This is the first parameter to recallOnline method. It is a string item in the current interaction
     * @param recommendedList  This is the second parameter to recallOnline method. It is an Arraylist of the recommended items
     * @return Integer This returns online recall for the current interaction.
     */
    Integer recallOnline(String consumedItem, ArrayList<String> recommendedList){
      if (recommendedList.contains(consumedItem)){
          //1/1
          return 1;
      }
      else {
          //0/1
          return 0;
      }
    }

    //because of online case it is identical to recall
    /**
     * This method is used to calculate the clickThrough considering prequential evaluation accepting two parameters
     * ClickThrough = 1 if E∩L ̸= 0/  and zero otherwise
     * @param consumedItem This is the first parameter to clickThrough method. It is a string item in the current interaction
     * @param recommendedList  This is the second parameter to clickThrough method. It is an Arraylist of the recommended items
     * @return Integer This returns online clickThrough for the current interaction.
     */
    Integer clickThrough(String consumedItem, ArrayList<String> recommendedList){
        if (recommendedList.contains(consumedItem)){
            return 1;
        }
        else {
            return 0;
        }
    }


    /**
     * This method is used to calculate the reciprocalRank considering prequential evaluation accepting two parameters
     * reciprocalRank is considering the position of the relevant item
     * @param consumedItem This is the first parameter to reciprocalRank method. It is a string item in the current interaction
     * @param recommendedList  This is the second parameter to reciprocalRank method. It is an Arraylist of the recommended items
     * @return Float This returns online reciprocalRank for the current interaction.
     */
    Float reciprocalRank(String consumedItem, ArrayList<String> recommendedList){
        if (recommendedList.contains(consumedItem)){
            Integer rank = recommendedList.indexOf(consumedItem);
            return (float)1/rank;
        }
        else {
            return 0f;
        }
    }


    /**
     * This method is used to calculate the DisCumulativeGain considering prequential evaluation accepting two parameters
     * Discounted cumulative gain DCG@K =  ∑ rel(ik)/log (1+k)
     * @param consumedItem This is the first parameter to DisCumulativeGain method. It is a string item in the current interaction
     * @param recommendedList  This is the second parameter to DisCumulativeGain method. It is an Arraylist of the recommended items
     * @return Double This returns online DisCumulativeGain for the current interaction.
     */
    Double DisCumulativeGain(String consumedItem, ArrayList<String> recommendedList){

        if (recommendedList.contains(consumedItem)){
            Integer k = recommendedList.indexOf(consumedItem);
            Double rank = (double)1+k;
            return 1/(Math.log(rank)/Math.log(2));
        }
        else {
            return 0d;
        }
    }












}
