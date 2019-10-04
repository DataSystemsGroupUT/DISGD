import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DISGD {
    public static void main(String[] args) throws Exception {

        //Integer nodesWhereUserAppearance = 2;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(4);
        DataStream<Tuple3<String, String, Float>> inputStream = env.addSource(new SourceWithTimestamp("/Users/heidyhazem/Desktop/ITBSGDN/nff.txt"));
        final OutputTag<Tuple4<Integer, String, String, Map<String, Float>>> scoreMapOutput = new OutputTag<Tuple4<Integer, String, String, Map<String, Float>>>("scoreMap-output") {
        };


        if (inputStream == null) {
            System.exit(1);
            return;
        }

        //**********************************hash the stream according to v.simple distributing technique********************************
        //https://docs.google.com/presentation/d/1QIbTYqAX_zNno8xCBicMshQF3_2nW1ZTKaxVLY5dj5U/edit?usp=sharing

        DataStream<Tuple4<Integer,String,String,Float>> KeyedRatingStream = inputStream.flatMap(new FlatMapFunction<Tuple3<String, String, Float>, Tuple4<Integer, String, String, Float>>() {

            //Assign any numbers of nodes but it should be even.
            //n_i is the replication factor of the vectors(asynchronous)
            Integer ni = 6;
            Integer interactionHashNumber;

            @Override
            public void flatMap(Tuple3<String, String, Float> input, Collector<Tuple4<Integer, String, String, Float>> out) throws Exception {
                ArrayList<Integer> usersNodes = new ArrayList<>();
                ArrayList<Integer> itemNodes = new ArrayList<>();

                //Hash user
                //ni = userId%n + (0:n-1)n
                Long userID = Long.parseLong(input.f0);
                Long userHashNumber = userID % ni;
                for (int x = 0; x <= ni - 1; x++) {
                    Integer k = (int) (userHashNumber + (x * ni));
                    usersNodes.add(k);
                    //out.collect(Tuple5.of(k,"user",input.f1,input.f2,poissonNumber));
                }


                //hash item
                //nodes = (itemId%n)n + (0:n-1)
                Long itemID = Long.parseLong(input.f1);
                Long itemHashNumber = itemID % ni;
                for (int x = 0; x <= ni - 1; x++) {
                    Integer k = (int) ((itemHashNumber * ni) + x);
                    itemNodes.add(k);
                    //out.collect(Tuple5.of(k,"item",input.f1,input.f2,poissonNumber));
                }

                //get the common hash number and assign it
                for (Integer useNo : usersNodes) {
                    for (Integer itemNo : itemNodes) {
                        if (useNo.equals(itemNo)) {
                            interactionHashNumber = useNo;
                            break;
                        }
                    }
                }
                out.collect(Tuple4.of(interactionHashNumber, input.f0, input.f1, input.f2));
            }
        });

        //*************************************************************************************************************************



        DataStream<String> bagUpdateStream = KeyedRatingStream.keyBy(0)
                .keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, String>() {

                    MapState<String, Double[]> itemsMatrix;
                    MapState<String, Double[]> usersMatrix;
                    MapState<String, ArrayList<String>> ratedItemsByUser;
                    ValueState<Integer> flagForInitialization;

                    int latentFeatures = 10;
                    Double lambda = 0.01;
                    Double mu = 0.05;

                    //number of recommended items
                    Integer N = 10;

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<String> out) throws Exception {
                        //Matrix factorization for each bag
                        String user = input.f1;
                        String item = input.f2;


                        SGD sgd = new SGD();
                        userItem userItemVectors;

                        Double[] itemVector;
                        Double[] userVector;
                        Boolean knownUser = false;

                        ArrayList<String> recommendedItems;

                        //Map<String,Float> itemsScoresMatrixMap = new HashMap<>();
                        Map<String, Float> itemsScoresMatrixMap = new HashMap<>();


                        //************************get or initialize user vector************************
                        if (usersMatrix.contains(user)) {
                            userVector = usersMatrix.get(user);
                            knownUser = true;
                        }
                        //user is not known before.
                        //initialize it
                        else {
                            //initialize with static random numbers generated with respect to gaussian distribution from PoissonGenerator
                            //mean=0 and variance=1
                            userVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //or it can be generated every time
                            //userVector = VectorOperations.initializeVector(latentFeatures);
                        }
                        //******************************************************************************
                        //************************get or initialize item vector*************************

                        if (itemsMatrix.contains(item)) {
                            itemVector = itemsMatrix.get(item);
                        }
                        //item is not known before.
                        //initialize it
                        else {

                            itemVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //itemVector = VectorOperations.initializeVector(latentFeatures);
                        }
                        //******************************************************************************
                        //*******************************1-recommend top k items for the user*****************************************
                        //rate the coming user with all items
                        //output it on the side

                        Iterable<Map.Entry<String, Double[]>> itemsVectors = itemsMatrix.entries();
                        for (Map.Entry<String, Double[]> AnItem : itemsVectors) {
                            Double score = Math.abs(1 - VectorOperations.dot(userVector, AnItem.getValue()));
                            itemsScoresMatrixMap.put(AnItem.getKey(), score.floatValue());
                        }
                        //************************************************************************************************************
                        //*******************************2-Score the recommendation list given the true observed item i***************
                        //send the maps to take the average then recommend and score
                        context.output(scoreMapOutput, Tuple4.of(input.f0, user, item, itemsScoresMatrixMap));

                        // }
                        //*******************************3. update the model with the observed event**************************************

                                userItemVectors = sgd.update_isgd2(userVector, itemVector, mu, lambda);
                                usersMatrix.put(user, userItemVectors.userVector);
                                itemsMatrix.put(item, userItemVectors.itemVector);

                        //****************************************************************************************************************
                    }

                    @Override
                    public void open(Configuration config) {

                        MapStateDescriptor<String, Double[]> descriptor2 =
                                new MapStateDescriptor<>(
                                        "itemMatrixDescriptor",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, Double[]> descriptor22 =
                                new MapStateDescriptor<>(
                                        "usersMatrix",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, ArrayList<String>> descriptor3 =
                                new MapStateDescriptor<>(
                                        "ratedItemsByUser",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<ArrayList<String>>() {
                                        })
                                );

                        ValueStateDescriptor<Integer> descriptor4 =
                                new ValueStateDescriptor<Integer>(
                                        "flag",
                                        Integer.class,
                                        0
                                );

                        itemsMatrix = getRuntimeContext().getMapState(descriptor2);
                        usersMatrix = getRuntimeContext().getMapState(descriptor22);
                        ratedItemsByUser = getRuntimeContext().getMapState(descriptor3);
                        flagForInitialization = getRuntimeContext().getState(descriptor4);
                    }

                });


        DataStream<Tuple4<Integer, String, String, Map<String, Float>>> avgMapScoreStream = ((SingleOutputStreamOperator<String>) bagUpdateStream).getSideOutput(scoreMapOutput);

        DataStream<Integer> recallStream = avgMapScoreStream.keyBy(1)
                .countWindow(1)
                .process(new ProcessWindowFunction<Tuple4<Integer, String, String, Map<String, Float>>, Integer, Tuple, GlobalWindow>() {
                    MapState<String, Void> ratedItems;

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple4<Integer, String, String, Map<String, Float>>> iterable, Collector<Integer> out) throws Exception {
                        Integer counterCheckNode = 0;
                        Map<String, Float> AllitemsWithScore = new HashMap<>();
                        String currentUser = iterable.iterator().next().f1;
                        String currentItem = iterable.iterator().next().f2;

                        //************************************Avg Scores************************************************************************
                        for (Tuple4<Integer, String, String, Map<String, Float>> userscoreMatrix : iterable) {
                            counterCheckNode += 1;
                            //get the sum of score(preparing for the average)
                            for (Map.Entry<String, Float> userItemScore : userscoreMatrix.f3.entrySet()) {
                                //test first with all items
                                if (ratedItems.contains(userItemScore.getKey())) {
                                    continue;
                                } else {
                                    AllitemsWithScore.put(userItemScore.getKey(), userItemScore.getValue());
                                }

                            }
                        }

                        //************************************************************************************************************
                        //here we are ready for scoring and recommendation
                        //*******************************2-Score the recommendation list given the true observed item i***************
                        ArrayList<String> recommendedItems = new ArrayList<>();
                        Integer N = 10;
                        Integer recall;
                        Evaluation eval = new Evaluation();
                        AllitemsWithScore.entrySet().stream().sorted(Map.Entry.comparingByValue())
                                .limit(N)
                                .forEach(itemRate -> recommendedItems.add(itemRate.getKey()));

                        recall = eval.recallOnline(currentItem, recommendedItems);
                        out.collect(recall);
                        //updated rated items list( )
                        ratedItems.put(currentItem, null);
                        //************************************************************************************************************
                    }

                    @Override
                    public void open(Configuration config) {

                        MapStateDescriptor<String, Void> descriptor =
                                new MapStateDescriptor<>(
                                        "ratedItems",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Void>() {
                                        })
                                );

                        ratedItems = getRuntimeContext().getMapState(descriptor);
                    }

                });


        recallStream.writeAsText("/Users/heidyhazem/Desktop/ITBSGDN/Bagging/recall22.txt", FileSystem.WriteMode.OVERWRITE);
        env.execute("semi Bagged Incremental stochastic gradient descent");

    }
}