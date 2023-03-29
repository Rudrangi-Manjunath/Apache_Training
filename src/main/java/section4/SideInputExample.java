package section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class SideInputExample {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, String>> preturn = pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\return.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] line = c.element().split(",");
                        c.output(KV.of(line[0], line[1]));
                    }
                }));

        PCollectionView<Map<String, String>> pMap = preturn.apply(View.asMap());
        pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\cust_order.csv"))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String line[] = c.element().split(",");
                        Map<String, String> pInsideMap = c.sideInput(pMap);
                        String returnCustomerName = pInsideMap.get(line[0]);
                        if (returnCustomerName == null) {
                            c.output(c.element());
                            System.out.println(c.element());
                        }
                    }
                }).withSideInputs(pMap));
        pipeline.run();
    }
}
