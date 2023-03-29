package section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CountExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> presult = pipeline.apply("ReadLines", TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\Count.csv"));
        PCollection<Long> poutput = presult.apply(Count.globally());
        poutput.apply(ParDo.of(new DoFn<Long, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(c.element().toString());
                System.out.println(c.element());
            }
        }));

        pipeline.run();
    }
}
