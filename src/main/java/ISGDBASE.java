import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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

public class ISGDBASE {
    public static void  main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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


        final OutputTag<Integer> outputTagRecall = new OutputTag<Integer>("recall"){};

        //Generate key 0 to all the stream to leverage keyed state
        DataStream<Tuple4<Integer,String,String,Float>> withKeyStream = inputStream.flatMap(new FlatMapFunction<Tuple3<String, String, Float>, Tuple4<Integer, String, String, Float>>() {
            @Override
            public void flatMap(Tuple3<String, String, Float> input, Collector<Tuple4<Integer, String, String, Float>> collector) throws Exception {
                Integer key = 0;
                collector.collect(Tuple4.of(key,input.f0,input.f1,input.f2));
            }
        });

        //Return top 10 recommended items
        DataStream<Tuple2<String,ArrayList<String>>> outputStream = withKeyStream
                .keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple2<String, ArrayList<String>>>() {

                    MapState<String,Double[]> itemsMatrix;
                    MapState<String,Double[]> usersMatrix;
                    MapState<String,ArrayList<String>> ratedItemsByUser;
                    ValueState<Integer> flagForInitialization;

                    int latentFeatures = 10;
                    Double lambda = 0.01;
                    Double mu = 0.05;
                    //number of recommended items
                    Integer N = 10;


                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple2<String, ArrayList<String>>> out) throws Exception {
                        SGD sgd = new SGD();
                        userItem userItemVectors;


                        Double[] itemVector;
                        Double[] userVector;

                        String user = input.f1;
                        String item = input.f2;
                        Boolean knownUser = false;

                        ArrayList<String> recommendedItems;

                        //initialize state with known items with normal distribution
                        if(flagForInitialization.value().equals(0)){

                            Double[] initializeVector ;

                            initializeVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};

                            GetItems getAllItems = new GetItems();

                            ArrayList<String> allItemsList = new ArrayList<>();
                            allItemsList = getAllItems.itemsList(params.get("input"));

                            for(String i : allItemsList){
                                //itemsMatrix.put(i,VectorOperations.initializeVector(latentFeatures));
                                itemsMatrix.put(i,initializeVector);
                            }
                            flagForInitialization.update(1);
                        }


                        if(usersMatrix.contains(user)){
                            userVector = usersMatrix.get(user);
                            knownUser = true;
                        }
                        //user is not know before.
                        //Add the user with his rated item in the state
                        else{

                            userVector = new Double[]{0.04412275, -0.03308702,  0.24307712, -0.02520921,  0.01096098,
                                    0.15824811, -0.09092324, -0.05916367,  0.01876032, -0.032987};
                            //userVector = VectorOperations.initializeVector(latentFeatures);
                        }


                        //itemVector = itemsMatrix.get(item);

                        if(itemsMatrix.contains(item)){
                            itemVector = itemsMatrix.get(item);

                        }
                        //user is not know before.
                        //Add the user with his rated item in the state
                        else{

                            itemVector = new Double[]{0.04412275, -0.03308702,  0.24307712, -0.02520921,  0.01096098,
                                    0.15824811, -0.09092324, -0.05916367,  0.01876032, -0.032987};
                            //itemVector = VectorOperations.initializeVector(latentFeatures);
                        }

                        if(knownUser) {

                            recommendedItems = sgd.recommend_isgd(itemsMatrix.entries(), userVector, ratedItemsByUser.get(user), N);
                            out.collect(Tuple2.of(user, recommendedItems));

                            //update ratedItemsByUser state
                            ArrayList<String> ratedItemList = ratedItemsByUser.get(user);
                            ratedItemList.add(item);
                            ratedItemsByUser.put(user,ratedItemList);

                            //****************************************************************************************************************************************************
                            //*******************************2-Score the recommendation list given the true observed item i*******************************************************
                            Evaluation evaluation = new Evaluation();
                            Integer recall = evaluation.recallOnline(item, recommendedItems);
                            context.output(outputTagRecall, recall);

                        }
                        else {
                            //if user is not known update rated items list by user direct
                            ArrayList<String> ratedItemListForUnknownUser = new ArrayList<>();
                            ratedItemListForUnknownUser.add(item);
                            ratedItemsByUser.put(user,ratedItemListForUnknownUser);
                        }
                        //****************************************************************************************************************************************************
                        //*******************************3. update the model with the observed event**************************************************************************
                        userItemVectors = sgd.update_isgd2(userVector,itemVector,mu,lambda);
                        usersMatrix.put(user,userItemVectors.userVector);
                        itemsMatrix.put(item,userItemVectors.itemVector);
                        //****************************************************************************************************************************************************
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

                        MapStateDescriptor<String,ArrayList<String>> descriptor3 =
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

        DataStream<Integer> recallSide =((SingleOutputStreamOperator<Tuple2<String,ArrayList<String>>>) outputStream).getSideOutput(outputTagRecall);

        recallSide.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        env.execute("ISGD Baseline");


    }
}