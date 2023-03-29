package section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class InMemoryExample {
    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        Pipeline p1 = Pipeline.create(options);

        PCollection<CustomerEntity> pCustomer = p1.apply(Create.of(getCustomers()));

        PCollection<String>poutput = pCustomer.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity cust) -> cust.getName()));

        poutput.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\output-inmemory.csv").withNumShards(1).withSuffix(".csv"));

        p1.run();
    }

    static List<CustomerEntity> getCustomers(){
        CustomerEntity c1 = new CustomerEntity("1","akash");
        CustomerEntity c2 = new CustomerEntity("2","prakash");
        List<CustomerEntity> list = Arrays.asList(c1,c2);
        return list;
    }
}
