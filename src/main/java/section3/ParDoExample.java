package section3;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class CustFilter extends DoFn<String,String>{

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();

        String arr[] = line.split(",");

        if(arr[3].equals("Los Angeles")) {
            c.output(line);
        }
    }
}


public class ParDoExample {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList =pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\customer_pardo.csv"));

        // Using ParDo
        PCollection<String>presult = pCustList.apply(ParDo.of(new CustFilter()));

        presult.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\output-pardo.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));

        pipeline.run();

    }

}


