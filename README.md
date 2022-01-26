# Java_8_streams
##### Objective:

Suppose you want to analyze the temperature data produced by weather stations (meteorological 
stations). 
Create a class WeatherStation with three attributes (fields): the city where the station is located, the 
station’s measurements (a list of objects of class Measurement), and a static field stations (a list of all 
existing weather stations – you need this list only for the next question). Also, create a class 
Measurement. Objects of class Measurement should have attributes time (an integer, representing the 
time of the measurement) and temperature (a double number).
Add a method maxTemperature(startTime, endTime) to class WeatherStation which returns the 
maximum temperature measured by this weather station between startTime and endTime.
Implement this method using Java 8 Stream operations, as far as possible. Also, add a main-method 
where you call your method using some test data (of your own choice) and print the result.

Add a method countTemperatures(t1,t2,r) to your class WeatherStation from the previous 
question. The method should return a list which contains two pairs of values: 1) temperature t1 paired 
with the number of times a temperature in the interval [t1-r..t1+r] has been measured so far by any 
of the weather stations in stations, and 2) temperature t2 paired with the number of times a 
temperature in the interval [t2-r..t2+r] has been measured so far by any of the weather stations in 
stations. Note that a single measurement contributes to both counts in case it is within both intervals.
It is sufficient for Question 2 to consider only the case that there are exactly two weather stations in list 
stations (a solution which works with an arbitrary number ≥ 2 of stations would not necessarily be more 
complex, and would of course be fine too as a solution, but it would not be worth additional marks). 
Considering only a single station would not be sufficient

Example: if there are two weather stations in list stations and the first station has measured 
temperatures 20.0, 11.7, -5.4, 18.7, 20.9 and the second station’s measurements are 8.4, 19.2, 7.2, then 
countTemperatures(19.0,10.8,2.1) should return the list ((19.0,4), (10.8,1)) (because there are in 
total (i.e., considering all stations) 4 measurements in the range between 19.0-2.1 and 19.0+2.1 and 1 
measurement in the range from 10.8-2.1 to 10.8+2.1).
For computing the result, you need to use an “emulated” MapReduce approach, as far as possible. That 
is, your code should resemble the MapReduce approach but using only plain Java >=8 (i.e., without using 
any actual MapReduce framework such as Hadoop). Also, you need to make use of Java 8 Streams (as far 
as possible) and parallel stream processing (where appropriate).
Finally, add code to your main-method which calls your countTemperatures method using some test 
measurement data from two different stations, and prints the result
