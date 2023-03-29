package section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;

public class DistinctExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> presult = pipeline.apply("ReadLines", TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\Distinct.csv"));
        PCollection<String> poutput = presult.apply(Distinct.create());
        poutput.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\DistinctOutput.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
