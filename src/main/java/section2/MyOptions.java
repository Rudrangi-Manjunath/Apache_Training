package section2;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
    void setInputFile(String file);
    String getInputFile();

    void setOutputFile(String file);
    String getOutputFile();

    void setExtn(String Extn);
    String getExtn();
}
