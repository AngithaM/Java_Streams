/*
 * Name: Angitha Mathew
 */
package LSDA_ASSIGNMENT2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.AbstractMap.SimpleEntry;

//Creating new class Measurement as described
public class WeatherStation {
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
	// method to return the max temperature between a range of time.
	public Measurement maxTemperature(int startTime, int endTime) {
		// Filtered list of temperatures: filtered by startTime and endTime.Max value of
		// the filtered list is taken by using stream and then the terminal operation
		// max
		Measurement maxMe = this.getMeasurements().stream()
				.filter(FilterStation -> (FilterStation.getTime() >= startTime))
				.filter(FilterStation -> (FilterStation.getTime() <= endTime))
				.max(Comparator.comparing(Measurement::getTemperature)).orElseThrow(IllegalStateException::new);
		//returning the measurement object, the temperature can also be returned but the entire object is returned to 
		//print in details
		return maxMe;
	}
	// To return a list of pairs containing the temperature and the count of the temperature within the range of+/- r of it.
	public static java.util.List<java.util.Map.Entry<Double, Integer>> countTemperature(double t1, double t2, int r) {

		//Count of t1 temperature range: Stations is streamed parallely and then flattened (to create a
		//list irrespective of the weatherstation).Then the list is filtered to produce a smaller list which has temperature
		// between t1+r and t1-r. This list is converted to a hash map with the temperture and a count 1. This count is then summed up
		//to return the final count wrt t1.		

		int countOfT1 = stations.parallelStream().flatMap(station -> station.getMeasurements().parallelStream())
				.filter(filterList -> Math.abs(t1-filterList.getTemperature()) <= r)
				.collect(Collectors.toMap(M2 -> M2.getTemperature(), M2 -> 1, (u, v) -> {
					throw new IllegalStateException();
				}, HashMap::new)).entrySet().stream().map(m -> m.getValue()).reduce(0, Integer::sum);

		//Count of t2 temperature range: same logic as with t1.
		int countOfT2  = stations.stream().flatMap(station -> station.getMeasurements().stream())
				.filter(filterList -> Math.abs(t2-filterList.getTemperature()) <= r)
				.collect(Collectors.toMap(M2 -> M2.getTemperature(), M2 -> 1, (u, v) -> {
					throw new IllegalStateException();
				}, HashMap::new)).entrySet().stream().map(m -> m.getValue()).reduce(0, Integer::sum);
		//The two temperature values passed and	it's respective count is transformed into a list of pairs as requested in the 
		// assgnment and returned back to the main function to be printed and displayed.
		java.util.List<java.util.Map.Entry<Double, Integer>> pairList = new java.util.ArrayList<>();
		Entry<Double, Integer> ansList1 = new SimpleEntry<>(t1, countOfT1);
		Entry<Double, Integer> ansList2 = new SimpleEntry<>(t2, countOfT2);
		pairList.add(ansList1);
		pairList.add(ansList2);
		return pairList;

	}

	// Test Data to call for Q1 and Q2
	public static void main(String Args[]) {
		// Using streams to initialize the lists of Measurement with values for
		// testing.(The question did say use streams as much as possible.)
		List<Measurement> measureList1 = Stream
				.of(new Measurement(1, 20.0), new Measurement(4, 11.7), new Measurement(8, -5.4),
						new Measurement(12, 18.7), new Measurement(20, 20.9))
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
		int startTime;
		int endTime;
		// Finding the max temperature for the entire day on station 1
		Measurement maxTempMe1= weatherS_1.maxTemperature(startTime=1, endTime=24);
		// Printing results of the maxTemperature
		System.out.println("\n Maximum Temperature measured by the weatherstation between " + startTime + " and " + endTime
				+ " is: " + maxTempMe1.getTemperature() + " at " + maxTempMe1.getTime() + "th hour");
		// Finding the max temperature between noon and midnight on station 2
		Measurement maxTempMe2=weatherS_2.maxTemperature(startTime=12, endTime=24);
		// Printing results of the maxTemperature
		System.out.println("\n Maximum Temperature measured by the weatherstation between " + startTime + " and " + endTime
				+ " is: " + maxTempMe2.getTemperature() + " at " + maxTempMe2.getTime() + "th hour");
		// Should return a pair of list: using pair list instead of hash map to avoid
		// key-value dependency
		java.util.List<java.util.Map.Entry<Double, Integer>> pairlist = new java.util.ArrayList<>();
		// Counting the temperature between t1+/-r and t2+-r
		pairlist = countTemperature(19.0, 10.8, 2);
		// Printing out the pair List for each of the temperature passed.
		System.out.println("\n The temperature and count of it from all weatherstations are format[temp=count]:");
		System.out.println(Arrays.asList(pairlist));

	}
}

//Creating new class Measurement as described in Q1
class Measurement {
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
