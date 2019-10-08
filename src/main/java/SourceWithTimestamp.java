
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;


public class SourceWithTimestamp  implements SourceFunction<Tuple3<String,String,Float>>{

    //This to read from source of text file formatted in <(user)String,(item)String,(rate)Float>

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private boolean running=true;
    private String filePath;
    private int no_records =  0;

    public SourceWithTimestamp(String path)
    {
        filePath = path;

    }

    public SourceWithTimestamp(String path,int records)
    {
        filePath = path;
        no_records = records;

    }


    @Override
    public void run(SourceContext<Tuple3<String,String,Float>> sourceContext) throws Exception
    {
        try
        {
            int recordsEmitted=0;
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            line = reader.readLine();
            if(no_records == 0){

                while (running && line != null )
                {
                    String[] splittedWord = line.split(",");
                    String user_id = splittedWord[0];
                    String item_id = splittedWord[1];
                    Float rating = Float.parseFloat(splittedWord[2].trim());
                    Long timestamp = Long.parseLong(splittedWord[3].trim());
                    sourceContext.collectWithTimestamp(new Tuple3<String, String, Float>(user_id, item_id, rating), timestamp);
                    line = reader.readLine();

                    }

            }
            else {

                while (running && line != null && recordsEmitted <= no_records)
                {
                    String[] splittedWord = line.split(",");
                    String user_id = splittedWord[0];
                    String item_id = splittedWord[1];
                    Float rating = Float.parseFloat(splittedWord[2].trim());
                    Long timestamp = Long.parseLong(splittedWord[3].trim());



                    sourceContext.collectWithTimestamp(new Tuple3<String, String, Float>(user_id, item_id, rating), timestamp);
                    recordsEmitted++;
                    line = reader.readLine();
                }
            }

            reader.close();
        }
        catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}

