package section5;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class InnerJoinExample {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        // STEP 1: Read the two input files and create two PCollection<KV<String,String>>

        PCollection<KV<String, String>> pOrderCollection = pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\user_order.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] line = c.element().split(",");
                        String key = line[0];
                        String value = line[1] + "," + line[2] + "," + line[3];
                        // System.out.println(key+" "+value);
                        c.output(KV.of(key, value));
                    }
                }));

        PCollection<KV<String, String>> pUserCollection = pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\p_user.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] line = c.element().split(",");
                        String key = line[0];
                        String value = line[1];
                        // System.out.println(key+" "+value);
                        c.output(KV.of(key, value));
                    }
                }));

        // STEP 2: Creating two tuple tags

        TupleTag<String> orderTuple = new TupleTag<String>() {
        };
        TupleTag<String> userTuple = new TupleTag<String>() {
        };

        // combine datasets using coGroupByKey
        PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(orderTuple, pOrderCollection)
                .and(userTuple, pUserCollection)
                .apply(CoGroupByKey.create());

        // Process the CoGbkResult and output the combined data
      /*  result.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String key = c.element().getKey();
                Iterable<String> orderValues = c.element().getValue().getAll(orderTuple);
                Iterable<String> userValues = c.element().getValue().getAll(userTuple);
                   for (String order : orderValues) {
                        for (String user : userValues) {
                            c.output(key + " " + user + " " + order);
                        }
                    }
            }
        }));*/


        // iterate CoGbkResult and build String
        PCollection<String> Output = result.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String key = c.element().getKey();
                CoGbkResult valObject = c.element().getValue();
                Iterable<String> orderTable = valObject.getAll(orderTuple);
                Iterable<String> userTable = valObject.getAll(userTuple);
                for (String order : orderTable) {
                    for (String user : userTable) {
                        c.output(key + "," + user + "," + order);
                    }
                }

            }
        }));
        // save output to file
        Output.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\InnerJoinOutput.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}

