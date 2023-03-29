package section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


class StringtoKV extends DoFn<String, KV<String, Integer>> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        String line[] = c.element().split(",");
        c.output(KV.of(line[0], Integer.valueOf(line[3])));
    }
}

class KVToSTring extends DoFn<KV<String, Iterable<Integer>>, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Iterable<Integer> values = c.element().getValue();
        Integer sum = 0;
        for (Integer i : values) {
            sum += i;
        }
        c.output(key + " " + sum.toString());
    }
}

public class GroupByKeyExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pinput = pipeline.apply("ReadLines", TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\GroupByKey_data.csv"));

        PCollection<KV<String, Integer>> KvOrder = pinput.apply(ParDo.of(new StringtoKV()));

        // applying groupbykey

        PCollection<KV<String, Iterable<Integer>>> KvOrder2 = KvOrder.apply(GroupByKey.<String, Integer>create());

        PCollection<String> output = KvOrder2.apply(ParDo.of(new KVToSTring()));

        output.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\GroupByKeyOutput.csv").withNumShards(1).withSuffix(".csv"));

        pipeline.run();
    }
}
