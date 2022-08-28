


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;



public class TraceProducerPerSecWorkload {
    // private static final Logger log = LogManager.getLogger(KafkaProducerConfig.class);
    private static String cvsSplitBy = ",";

    private static double inputXPointValue;
    private static double targetXPointValue;

    public ArrayList<Double> getDatay() {
        return datay;
    }
    private static ArrayList<Double> datay = new ArrayList<Double>();
    public ArrayList<Double> getDatax() {
        return datax;
    }
    private static ArrayList<Double> datax = new ArrayList<Double>();
    public TraceProducerPerSecWorkload() throws IOException, URISyntaxException {
        this.loadWorkload();
    }
    private void loadWorkload() throws IOException, URISyntaxException {
        ClassLoader CLDR = this.getClass().getClassLoader();
        InputStream inputStream = CLDR.getResourceAsStream("the1.csv");
        List<String> out = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                out.add(line);
            }
        }
        double second=0;
        for (String line : out) {
            String[] workFields = line.split(cvsSplitBy);
            inputXPointValue = second;
            targetXPointValue = Double.parseDouble(workFields[1]);
            datax.add(inputXPointValue);
            datay.add(targetXPointValue);
            second++;

        }
    }
}
