import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;

public class Evaluation {

    //*** Evaluation class includes the evaluation methods for online recommender systems**//

    //precisionOnline caluclates online precision = |E ∩ L| / k
    Float precisionOnline(String consumedItem, ArrayList<String> recommendedList){
        Integer k = recommendedList.size();
        if (recommendedList.contains(consumedItem)){
            return (float)1/k;
        }
        else {
            return 0f;
        }
    }

    //recallOnline calculates online Recall = |E ∩ L| / |E|
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
    //ClickThrough = 1 if E∩L ̸= 0/  and zero otherwise;
    Integer clickThrough(String consumedItem, ArrayList<String> recommendedList){
        if (recommendedList.contains(consumedItem)){
            return 1;
        }
        else {
            return 0;
        }
    }

    //reciprocalRank is considering the position of the relevant item
    Float reciprocalRank(String consumedItem, ArrayList<String> recommendedList){
        if (recommendedList.contains(consumedItem)){
            Integer rank = recommendedList.indexOf(consumedItem);
            return (float)1/rank;
        }
        else {
            return 0f;
        }
    }

    //Discounted cumulative gain DCG@K =  ∑ rel(ik)/log (1+k)
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
