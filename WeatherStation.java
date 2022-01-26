/*
 * Name: Angitha Mathew
 * Classes WeatherStation and Measurement kept as it is. Only method is the new method countTemperature(t)
. All other methods removed for readability.
 */

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//Creating new class Measurement as described
public class WeatherStation implements java.io.Serializable {// implements Serializable to enable the object of the class
    //to be made serializable while using parallelize method
    // Adding the 3 attributes city, Measurements and stations(static) as described.
    private String city;
    public static List<WeatherStation> stations = new ArrayList<>();
    private List<Measurement> Measurements;

    /**
     * Constructor for WeatherStation object that takes in String and
     * List<Measurement>, and initialize the object.
     *
     * @return WeatherStation
     */
    public WeatherStation(String city, List<Measurement> Measurements) {
        this.city = city;
        this.Measurements = Measurements;
    }

    /**
     * Overriding the toString() method to print values of WeatherStation object.
     *
     * @return String
     */
    @Override
    public String toString() {
        return "WeatherStation [city=" + city + ", Measurements=" + Measurements + "]";
    }

    /**
     * Adding setter for the variable city
     *
     * @return void
     */
    public void setCity(String city) {
        this.city = city;
    }

    /**
     * Adding getter for the variable city
     *
     * @return String
     */
    public String getCity() {
        return city;
    }

    /**
     * Adding getter for the variable Measurements
     *
     * @return List<Measurement>
     */
    public List<Measurement> getMeasurements() {
        return Measurements;
    }

    /**
     * Adding setter for the variable Measurements
     *
     * @return void
     */
    public void setMeasurements(List<Measurement> Measurements) {
        this.Measurements = Measurements;
    }

    // the new static method to count the temperatures that are approximate to temp in all the weather stations
    public static int countTemperature(int temp) {

        System.setProperty("hadoop.home.dir", "C:/winutils");//so that we can avoid setting the environment variable HADOOP_HOME manually
        SparkConf sparkConf = new SparkConf().setAppName("WordCount")//sets the spark configuration with default values
                // expect for the ones manually set like AppName
                .setMaster("local[4]").set("spark.executor.memory", "1g");//Sets maximum heap size

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);//JavaSparkContext object created: this is Java friendly and returns Java RDDs
        JavaRDD<WeatherStation> WS = ctx.parallelize(stations);//using the JavaSparkContext we distribute a collection to form JavaRDD

        List<Measurement> MS = new ArrayList<>();
        for (WeatherStation w : WS.collect()) {// we use .collect() to turn RDD into list
            MS.addAll(w.getMeasurements());//extract measurements from the list
        }
        JavaRDD<Measurement> MSRDD = ctx.parallelize(MS);// convert list into rdd
        MSRDD = MSRDD.filter(t -> (t.getTemperature() <= (temp + 1))).filter(t1 -> (t1.getTemperature() >= (temp - 1)));//filter the list according to temp +/- 1
        //Just printing all the filtered temperatures: Just for easy verification.
        for (Measurement fms : MSRDD.collect()) {
            System.out.println("Filtered temperatures between +/_ 1 of " + temp + " includes :" + fms.getTemperature());
        }
        int count = (int) MSRDD.count();// since we can't use MSRDD after closing the SparkContext, a new variable is created for storage
        ctx.stop();
        ctx.close();
        return count;
    }

    // Test Data to call for Q1 of Assignment 3
    public static void main(String Args[]) {
        // Using streams to initialize the lists of Measurement with values for
        // testing.(The question did say use streams as much as possible.)
        List<Measurement> measureList1 = Stream
                .of(new Measurement(1, 20.0), new Measurement(4, 11.7), new Measurement(8, -5.4),
                        new Measurement(12, 18.7), new Measurement(20, 19.9))
                .collect(Collectors.toCollection(ArrayList::new));
        List<Measurement> measureList2 = Stream
                .of(new Measurement(1, 8.4), new Measurement(14, 19.2), new Measurement(23, 7.2))
                .collect(Collectors.toCollection(ArrayList::new));
        // Passing the two lists as well as the city names to the WeatherStation
        // constructor.
        WeatherStation weatherS_1 = new WeatherStation("Galway", measureList1);
        WeatherStation weatherS_2 = new WeatherStation("Cork", measureList2);
        // adding this to stations- the static variable which hold the list of stations.
        // You can add more than two with no change to the logic in case there are more
        // stations.
        stations.add(weatherS_1);
        stations.add(weatherS_2);
        // Printing the station details.(Just to make the results easier to verify.)
        System.out.println("The list of weather Stations are:");
        stations.forEach(s -> System.out.println(s.toString()));
        int temp_t = 19;
        //Calling the new method described in the question.
        int count = countTemperature(temp_t);
        System.out.println("The number of times temperature t : " + temp_t + " has been approximately measured so far by any of the weather stations is " + count);


    }
}

//Creating new class Measurement as described in earlier assignment
class Measurement implements java.io.Serializable {// implements Serializable to enable the object of the class
    //to be made serializable while using parallelize method
    // Adding the 2 attributes time(representing 0-24 hrs) and temperature of type
    // int and double respectively.
    private int time;
    private double temperature;

    /**
     * Adding getter for the variable time
     *
     * @return int time
     */
    public int getTime() {
        return time;
    }

    /**
     * Adding setter for the variable time
     *
     * @return void
     */
    public void setTime(int time) {
        this.time = time;
    }

    /**
     * Adding getter for the variable temperature
     *
     * @return temperature
     */
    public double getTemperature() {
        return temperature;
    }

    /**
     * Adding setter for the variable temperature
     *
     * @return void
     */
    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    /**
     * Overriding the toString() method to print values of Measurement object.
     *
     * @return String
     */
    @Override
    public String toString() {
        return "Measurement [time=" + time + ", temperature=" + temperature + "]";
    }

    /**
     * Constructor for Measurement object that takes in int and Double and
     * initialize the object.
     *
     * @return Measurement
     */
    public Measurement(int time, double temperature) {
        this.time = time;
        this.temperature = temperature;
    }

}
