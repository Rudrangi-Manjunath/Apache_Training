package section9;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

import java.util.stream.Collectors;

public class SQLJoinExample {
    final static String order_header = "userId,orderId,productId,Amount";
    final static Schema order_schema = Schema.builder()
            .addStringField("userId")
            .addStringField("orderId")
            .addStringField("productId")
            .addDoubleField("Amount").build();

    final static String user_header = "userId,name";
    final static Schema user_schema = Schema.builder()
            .addStringField("userId")
            .addStringField("name").build();

    final static Schema order_user_schema = Schema.builder()
            .addStringField("userId")
            .addStringField("orderId")
            .addStringField("productId")
            .addDoubleField("Amount").addStringField("name").build();

    public static class StringToOrderRow extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {

            if (!c.element().equalsIgnoreCase(order_header)) {
                String arr[] = c.element().split(",");

                Row record = Row.withSchema(order_schema).addValues(arr[0], arr[1], arr[2], Double.valueOf(arr[3])).build();
                c.output(record);
            }
        }
    }

    public static class StringToUserRow extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {

            if (!c.element().equalsIgnoreCase(user_header)) {
                String arr[] = c.element().split(",");

                Row record = Row.withSchema(user_schema).addValues(arr[0], arr[1]).build();
                c.output(record);
            }
        }
    }

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        // TODO: read csv file
        PCollection<String> order = pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\user_order.csv"));
        PCollection<String> user = pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\p_order.csv"));

        // TODO: convert to Row
        PCollection<Row> orderRow = order.apply(ParDo.of(new StringToOrderRow())).setRowSchema(order_schema);
        PCollection<Row> userRow = user.apply(ParDo.of(new StringToUserRow())).setRowSchema(user_schema);

        // TODO: Apply SQL.Transform query

        PCollection<Row> sqlInput = PCollectionTuple.of(new TupleTag<>("order"), orderRow)
                .and(new TupleTag<>("user"), userRow)
                .apply(SqlTransform.query("select o.*,u.name from orders o inner join users u on o.userId=u.userId"));

        // TODO: Row to String
        PCollection<String> sqlOutput = sqlInput.apply(ParDo.of(new DoFn<Row, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Row row = c.element();
                String record = row.getValues().stream().map(Object::toString).collect(Collectors.joining(","));
                c.output(record);
            }
        }));

        // TODO: Write to file
        sqlOutput.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\output\\sql_join").withSuffix(".csv").withNumShards(1));
        pipeline.run().waitUntilFinish();
    }
}
