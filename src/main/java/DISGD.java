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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DISGD {
    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final ParameterTool params = ParameterTool.fromArgs(args);
        Integer ni = Integer.valueOf(params.get("ni"));
        Integer nc = (int) Math.pow(ni,2);
        env.setParallelism(nc);

        DataStream<Tuple3<String, String, Float>> inputStream = null;


        if(params.has("input") && params.has("records")){
         inputStream = env.addSource(new SourceWithTimestamp(params.get("input"),params.getInt("records")));
        }
        else if(params.has("input") && !params.has("records")){
            inputStream = env.addSource(new SourceWithTimestamp(params.get("input")));
        }
        else {
            System.out.println("Use --input to specify file input  or use --records to specify records");
        }

        final OutputTag<Tuple4<Integer, String, String, Map<String, Float>>> scoreMapOutput = new OutputTag<Tuple4<Integer, String, String, Map<String, Float>>>("scoreMap-output") {
        };
        if (inputStream == null) {
            System.exit(1);
            return;
        }

        //**********************************hash the stream according to  replication and splitting mechanism********************************
        DataStream<Tuple5<Integer, String, String, Float, Integer>> KeyedRatingStream = inputStream.flatMap(new FlatMapFunction<Tuple3<String, String, Float>, Tuple5<Integer, String, String, Float, Integer>>() {

            Integer nodes = ni;
            Integer interactionHashNumber;

            @Override
            public void flatMap(Tuple3<String, String, Float> input, Collector<Tuple5<Integer, String, String, Float, Integer>> out) throws Exception {
                int poissonNumber;
                poissonNumber = PoissonGenerator.getPoisson(1);


                ArrayList<Integer> usersNodes = new ArrayList<>();
                ArrayList<Integer> itemNodes = new ArrayList<>();

                //Hash user
                //nodes = userId%n + (0:n-1)n
                Long userID = Long.parseLong(input.f0);
                Long userHashNumber = userID % nodes;
                for (int x = 0; x <= nodes - 1; x++) {
                    Integer k = (int) (userHashNumber + (x * nodes));
                    usersNodes.add(k);
                }

                //hash item
                //nodes = (itemId%n)n + (0:n-1)
                Long itemID = Long.parseLong(input.f1);
                Long itemHashNumber = itemID % nodes;
                for (int x = 0; x <= nodes - 1; x++) {
                    Integer k = (int) ((itemHashNumber * nodes) + x);
                    itemNodes.add(k);
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
                //304 to avoid skewness in case of nc=4
                out.collect(Tuple5.of(interactionHashNumber*304, input.f0, input.f1, input.f2, poissonNumber));
            }
        });


//*************************************************************************************************************************

        DataStream<String> bagUpdateStream = KeyedRatingStream.keyBy(0)
                .keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple5<Integer, String, String, Float, Integer>, String>() {

                    MapState<String, Double[]> itemsMatrix;
                    MapState<String, Double[]> usersMatrix;
                    MapState<String, ArrayList<String>> ratedItemsByUser;
                    ValueState<Integer> flagForInitialization;

                   // int latentFeatures = 10;
                    Double lambda = 0.01;
                    Double mu = 0.05;

                    //number of recommended items
                    Integer N = 10;

                    @Override
                    public void processElement(Tuple5<Integer, String, String, Float, Integer> input, Context context, Collector<String> out) throws Exception {
                        //Matrix factorization for each bag
                        String user = input.f1;
                        String item = input.f2;
                        Integer k = input.f4;

                        SGD sgd = new SGD();
                        userItem userItemVectors;

                        Double[] itemVector;
                        Double[] userVector;


                        Map<String, Float> itemsScoresMatrixMap = new HashMap<>();


                        //************************get or initialize user vector************************
                        if (usersMatrix.contains(user)) {
                            userVector = usersMatrix.get(user);
                        }
                        //user is not known before.
                        //initialize it
                        else {
                            userVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //or regenerate random gaussian initializations
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
                            //or regenerate random gaussian initializations
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
                        k = 1;
                        if (k > 0) {
                            //TODO: Add the iteration loop
                            for (Integer l = 0; l < k; l++) {
                                userItemVectors = sgd.update_isgd2(userVector, itemVector, mu, lambda);
                                usersMatrix.put(user, userItemVectors.userVector);
                                itemsMatrix.put(item, userItemVectors.itemVector);
                            }
                        }
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
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Map<String, Float>>, Integer>() {

                    MapState<String, Void> ratedItems;

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Map<String, Float>> input, Context context, Collector<Integer> out) throws Exception {
                        Map<String, Float> AllitemsWithScore = new HashMap<>();
                        String currentItem = input.f2;


                        //************************************Avg Scores************************************************************************

                        for (Map.Entry<String, Float> userItemScore : input.f3.entrySet()) {
                            //test first with all items
                            if (ratedItems.contains(userItemScore.getKey())) {
                                continue;
                            } else {
                                AllitemsWithScore.put(userItemScore.getKey(), userItemScore.getValue());
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

        if(params.has("output")){
            recallStream.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Use --output to specify file input ");
        }



        env.execute("Distributed Incremental stochastic gradient descent");

    }
}