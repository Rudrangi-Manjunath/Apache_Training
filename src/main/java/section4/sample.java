package section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class sample {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        PCollection<String> output = pipeline.apply(TextIO.read().from("C:\\Users\\asus\\OneDrive\\Desktop\\Results\\inputM.csv"));
        PCollection<String> output1;
        output1 = output.apply(MapElements.into(TypeDescriptors.strings()).via((String obj) -> obj.toUpperCase()));
        output1.apply(TextIO.write().to("C:\\Users\\asus\\OneDrive\\Desktop\\Results\\output1.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
