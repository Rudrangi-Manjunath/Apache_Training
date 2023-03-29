package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class MyCityPartition implements PartitionFn<String>{

    @Override
    public int partitionFor(String ele,int numPartitions) {

        String arr[] = ele.split(",");

        if(arr[3].equals("Los Angeles")) {
            return 0;
        }
        else if(arr[3].equals("Phoenix")) {
            return 1;
        }
        else {
            return 2;
        }
    }
}

public class PartitionExample {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList =pipeline.apply(TextIO.read().from("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\Partiton.csv"));

        PCollectionList<String>partition = pCustList.apply(Partition.of(3, new MyCityPartition()));

        PCollection<String> p0 = partition.get(0);
        PCollection<String> p1 = partition.get(1);
        PCollection<String> p2 = partition.get(2);
    }

}