package section3;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;


class MyFilter implements SerializableFunction<String,Boolean>{

    @Override
    public Boolean apply(String input) {

        return input.contains("Los Angeles");
    }

}

public class FilterExample {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList = pipeline
                .apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\customer_pardo.csv"));

        PCollection<String>presult = pCustList.apply(Filter.by(new MyFilter()));

        presult.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\output-MyFilter.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));

        pipeline.run();
    }

}
