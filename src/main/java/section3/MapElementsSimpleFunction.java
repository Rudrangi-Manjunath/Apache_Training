package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;


class User extends SimpleFunction<String,String>{

    @Override
    public String apply(String input) {

        String arr[] = input.split(",");

        String sex = arr[6];

        String output ="";

        if(sex.equals("1")) {
            output = arr[0]+","+arr[1]+","+arr[2]+","+arr[3]+","+arr[4]+","+arr[5]+","+"M";
        }
        else {
            output =arr[0]+","+arr[1]+","+arr[2]+","+arr[3]+","+arr[4]+","+arr[5]+","+"F";
        }

        return output;
    }


}


public class MapElementsSimpleFunction {


    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList =pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\user.csv"));

        // Converting lower case customer names to Uppercase

        // Using Simple Functions

        PCollection<String> poutput = pCustList.apply(MapElements.via(new User()));

        /*
         * PCollection<Void> presult =
         * poutput.apply(MapElements.into(TypeDescriptors.voids()).via((String s) ->{
         * System.out.println(s); return null; }));
         */
        poutput.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\output-simpleFunction.csv").withNumShards(1).withSuffix(".csv"));

        pipeline.run();

    }

}

