package section7;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JDBCIOExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> presult = pipeline.apply(JdbcIO.<String>read().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3000/reactlibrarydatabase?useSSL=false")
                        .withUsername("Local instance MySQL80")
                        .withPassword("2001@Manju"))
                .withQuery("select id,title,author from book where id = ?")
                .withCoder(StringUtf8Coder.of())
                .withStatementPreparator(new JdbcIO.StatementPreparator() {
                    @Override
                    public void setParameters(PreparedStatement preparedStatement) throws Exception {
                        preparedStatement.setInt(1, 1);
                    }
                })
                .withRowMapper(new JdbcIO.RowMapper<String>() {
                    @Override
                    public String mapRow(ResultSet resultSet) throws Exception {
                        return resultSet.getString(1) + "," + resultSet.getString(2) + "," + resultSet.getString(3);
                    }
                })
        );


        presult.apply(TextIO.write().to("C:\\Users\\LPC\\Documents\\Synergy\\Apache\\output\\jdbc-output.csv").withNumShards(1).withSuffix(".csv"));

        pipeline.run();

    }
}
