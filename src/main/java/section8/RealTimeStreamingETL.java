package section8;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.joda.time.Duration;

import java.awt.*;
import java.sql.PreparedStatement;

public class RealTimeStreamingETL {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<KV<Long, IOTEvent>> presult = pipeline.apply(KafkaIO.<Long, IOTEvent>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("beamtopic")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(IOTDeserializer.class)
                .withoutMetadata()
        );

        presult.apply(Values.<IOTEvent>create())
                .apply(Window.<IOTEvent>into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply(ParDo.of(new DoFn<IOTEvent, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        if (c.element().getTemperature() > 80.0) {
                            c.output(c.element().getDeviceId());
                        }

                    }
                }))

                .apply(Count.perElement())
                .apply(JdbcIO.<KV<String, Long>>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create("com.mysql.jdbc.Driver", "jdbc:mysql://127.0.0.1:3306/beamdb?useSSL=false")
                                .withUsername("root").withPassword("root"))
                        .withStatement("insert into event values (?,?) ")
                        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<String, Long>>() {

                            public void setParameters(KV<String, Long> element, PreparedStatement preparedStatement) throws Exception {
                                // TODO Auto-generated method stub
                                preparedStatement.setString(1, element.getKey());
                                preparedStatement.setLong(2, element.getValue());
                            }
                        })
                );

        pipeline.run().waitUntilFinish();

    }

}
