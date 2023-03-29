package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;


public class MapElementsExample {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList =pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\customer.csv"));

        // Converting lower case customer names to UpperCase

        // Using TypeDescriptors

        PCollection<String> poutput = pCustList.apply(MapElements.into(TypeDescriptors.strings()).via((String s) -> s.toUpperCase()));

        poutput.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\output-customer.csv").withNumShards(1).withSuffix(".csv"));


        /*
         * // Printing Each String in the PCollection PCollection<Void> presult =
         * poutput.apply(MapElements.into(TypeDescriptors.voids()).via((String s) ->{
         * System.out.println(s); return null; }));
         */


        pipeline.run();


    }

}

