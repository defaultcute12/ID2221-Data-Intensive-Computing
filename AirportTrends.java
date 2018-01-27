package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.scala.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.jute.compiler.JFloat;

import java.util.Calendar;
import java.util.TimeZone;



public class AirportTrends {

    public enum JFKTerminal {
        TERMINAL_1(71436),
        TERMINAL_2(71688),
        TERMINAL_3(71191),
        TERMINAL_4(70945),
        TERMINAL_5(70190),
        TERMINAL_6(70686),
        NOT_A_TERMINAL(-1);

        int mapGrid;

        Calendar calendar = Calendar.getInstance();

        private JFKTerminal(int grid){
            this.mapGrid = grid;
        }

        public static JFKTerminal gridToTerminal(int grid){
            for(JFKTerminal terminal : values()){
                if(terminal.mapGrid == grid) return terminal;
            }
            return NOT_A_TERMINAL;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get the taxi ride data stream - Note: you got to change the path to your local data file
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("/home/mallu/flink-training-exercises/nycTaxiRides.gz", 60, 2000));

        //Task1- Fetching Data Pattern terminal per hour
        SingleOutputStreamOperator<Tuple3<JFKTerminal, Integer, Integer>> terminalperhour;
        terminalperhour = rides
                //filter rides starting and ending in a terminal
                .filter(new FilterFunction<TaxiRide> (){
                    @Override
                    public boolean filter(TaxiRide taxiRide) throws Exception {
                        JFKTerminal startloc = JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat));
                        JFKTerminal endloc = JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat));
                        return taxiRide.isStart&&startloc != JFKTerminal.NOT_A_TERMINAL || !taxiRide.isStart&&endloc != JFKTerminal.NOT_A_TERMINAL;
                    }
                })
                //map coordinates to grid cells to terminal, d
                .map(new TerminalRideMatcher())
                // partition by terminal and hour
                .<KeyedStream<Tuple3<JFKTerminal, Integer, Integer>,
                Tuple3<JFKTerminal, Integer, Integer>>>keyBy(0, 2)
                // build sliding window every hour
                .timeWindow(Time.minutes(60), Time.minutes(60))
                // sumup rides for that terminal
                .sum(1);
            //Task2
            SingleOutputStreamOperator<Tuple3<JFKTerminal, Integer, Integer>> popularTerminal;
            popularTerminal = rides
                    //filter rides starting and ending in a terminal
                    .filter(new FilterFunction<TaxiRide> (){
                        @Override
                        public boolean filter(TaxiRide taxiRide) throws Exception {
                            JFKTerminal startloc = JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat));
                            JFKTerminal endloc = JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat));
                            return taxiRide.isStart&&startloc != JFKTerminal.NOT_A_TERMINAL || !taxiRide.isStart&&endloc != JFKTerminal.NOT_A_TERMINAL;
                        }
                    })
                    //map coordinates to grid cells to terminal, and Timestamp to hour
                    .map(new TerminalRideMatcher())
                    // partition by terminal and hour
                    .<KeyedStream<Tuple3<JFKTerminal, Integer, Integer>,
                Tuple3<JFKTerminal, Integer, Integer>>>keyBy(0, 2)
                    // build sliding window every hour
                    .timeWindow(Time.minutes(60), Time.minutes(60))
                    // sumup rides for that terminal
                    .sum(1)
                    //global window counting accross every terminal every hour
                    .timeWindowAll(Time.minutes(60))
                    //pick the terminal with max count as popular
                    .maxBy(1);

        terminalperhour.print();
        //popularTerminal.print();

        env.execute("Trend in Terminal Hourly");

    }

    /**
     * Map taxi ride to grid cell and event type.
     * Start records use departure location, end record use arrival location.
     */
    public static class TerminalRideMatcher implements MapFunction<TaxiRide, Tuple3<JFKTerminal,Integer, Integer>> {

        public Tuple3<JFKTerminal, Integer, Integer> map(TaxiRide taxiRide) throws Exception {
            //timestamp to hour
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            //start location
            if(taxiRide.isStart) {
                int gridCellId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat); //coord to grid cell
                JFKTerminal terminal = JFKTerminal.gridToTerminal(gridCellId);// grid cell to terminal
                calendar.setTimeInMillis(taxiRide.startTime.getMillis());
                Integer hour = calendar.get(Calendar.HOUR_OF_DAY);
                return new Tuple3<>(terminal, 1, hour);
            } else { //end location
                int  gridCellId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                JFKTerminal terminal = JFKTerminal.gridToTerminal(gridCellId);
                calendar.setTimeInMillis(taxiRide.endTime.getMillis());
                Integer hour = calendar.get(Calendar.HOUR_OF_DAY);
                return new Tuple3<>(terminal, 1, hour);
            }
        }

    }
}
