package section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class linetowords extends DoFn<String, String>{
    @ProcessElement
    public void processElement(ProcessContext c){
       String words[] = c.element().split(" ");
       for(String word:words){
           c.output(word);
       }
    }

}
public class Practise {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pinput = pipeline.apply("ReadLines", TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\sample.txt"));

        PCollection<String> pwords =pinput.apply(ParDo.of(new linetowords()));

        PCollection<KV<String,Long>> plist = pwords.apply(Count.perElement());

        PCollection<String> poutput = plist.apply(ParDo.of(new DoFn<KV<String,Long>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(c.element().getKey()+":"+c.element().getValue().toString());
                //System.out.println(c.element().getKey()+" "+c.element().getValue().toString());
            }
        }));

        poutput.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\output.txt").withNumShards(1).withSuffix(".txt"));

        pipeline.run();
    }
}
