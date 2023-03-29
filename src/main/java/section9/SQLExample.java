package section9;

import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.transforms.DoFn;

import org.apache.beam.sdk.transforms.ParDo;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;


public class SQLExample {
    final static String HEADER = "user_id,order_id,product_id,Amount";
    final static Schema schema = Schema.builder()
            .addStringField("user_id")
            .addStringField("order_id")
            .addStringField("product_id")
            .addDoubleField("Amount")
            .build();

    public static class StringToRow extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {

            if (!c.element().equalsIgnoreCase(HEADER)) {
                String arr[] = c.element().split(",");

                Row record = Row.withSchema(schema).addValues(arr[0], arr[1], arr[2], Double.valueOf(arr[3])).build();
                c.output(record);
            }
        }
    }


    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        // TODO: read csv file
        PCollection<String> fileInput = pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\user_order.csv"));

        // TODO: convert to Row
        PCollection<Row> rowInput = fileInput.apply(ParDo.of(new StringToRow())).setRowSchema(schema);

        // TODO: Apply SQL.Transform query

        PCollection<Row> sqlInput = rowInput.apply(SqlTransform.query("SELECT * from PCOLLECTION"));

        // TODO: convert PCollection<Row> to PCollection<String>
        PCollection<String> poutput = sqlInput.apply(ParDo.of(new DoFn<Row, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String outstring = c.element().getValues().stream().map(Object::toString).collect(Collectors.joining(","));
                c.output(outstring);
            }
        }));

        // TODO: write to csv file
        poutput.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\user_order_output.csv").withNumShards(1).withSuffix(".csv"));


        pipeline.run();

    }
}
