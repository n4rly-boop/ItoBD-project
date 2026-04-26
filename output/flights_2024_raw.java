// ORM class for table 'flights_2024_raw'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sun Apr 12 17:21:22 MSK 2026
// For connector: org.apache.sqoop.manager.PostgresqlManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.BooleanParser;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class flights_2024_raw extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("flight_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.flight_id = (Long)value;
      }
    });
    setters.put("year", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.year = (Integer)value;
      }
    });
    setters.put("month", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.month = (Integer)value;
      }
    });
    setters.put("day_of_month", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.day_of_month = (Integer)value;
      }
    });
    setters.put("day_of_week", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.day_of_week = (Integer)value;
      }
    });
    setters.put("fl_date", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.fl_date = (java.sql.Date)value;
      }
    });
    setters.put("op_unique_carrier", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.op_unique_carrier = (String)value;
      }
    });
    setters.put("op_carrier_fl_num", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.op_carrier_fl_num = (java.math.BigDecimal)value;
      }
    });
    setters.put("origin", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.origin = (String)value;
      }
    });
    setters.put("origin_city_name", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.origin_city_name = (String)value;
      }
    });
    setters.put("origin_state_nm", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.origin_state_nm = (String)value;
      }
    });
    setters.put("dest", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.dest = (String)value;
      }
    });
    setters.put("dest_city_name", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.dest_city_name = (String)value;
      }
    });
    setters.put("dest_state_nm", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.dest_state_nm = (String)value;
      }
    });
    setters.put("crs_dep_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.crs_dep_time = (Integer)value;
      }
    });
    setters.put("dep_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.dep_time = (java.math.BigDecimal)value;
      }
    });
    setters.put("dep_delay", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.dep_delay = (java.math.BigDecimal)value;
      }
    });
    setters.put("taxi_out", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.taxi_out = (java.math.BigDecimal)value;
      }
    });
    setters.put("wheels_off", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.wheels_off = (java.math.BigDecimal)value;
      }
    });
    setters.put("wheels_on", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.wheels_on = (java.math.BigDecimal)value;
      }
    });
    setters.put("taxi_in", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.taxi_in = (java.math.BigDecimal)value;
      }
    });
    setters.put("crs_arr_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.crs_arr_time = (Integer)value;
      }
    });
    setters.put("arr_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.arr_time = (java.math.BigDecimal)value;
      }
    });
    setters.put("arr_delay", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.arr_delay = (java.math.BigDecimal)value;
      }
    });
    setters.put("cancelled", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.cancelled = (Integer)value;
      }
    });
    setters.put("cancellation_code", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.cancellation_code = (String)value;
      }
    });
    setters.put("diverted", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.diverted = (Integer)value;
      }
    });
    setters.put("crs_elapsed_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.crs_elapsed_time = (java.math.BigDecimal)value;
      }
    });
    setters.put("actual_elapsed_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.actual_elapsed_time = (java.math.BigDecimal)value;
      }
    });
    setters.put("air_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.air_time = (java.math.BigDecimal)value;
      }
    });
    setters.put("distance", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.distance = (java.math.BigDecimal)value;
      }
    });
    setters.put("carrier_delay", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.carrier_delay = (Integer)value;
      }
    });
    setters.put("weather_delay", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.weather_delay = (Integer)value;
      }
    });
    setters.put("nas_delay", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.nas_delay = (Integer)value;
      }
    });
    setters.put("security_delay", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.security_delay = (Integer)value;
      }
    });
    setters.put("late_aircraft_delay", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        flights_2024_raw.this.late_aircraft_delay = (Integer)value;
      }
    });
  }
  public flights_2024_raw() {
    init0();
  }
  private Long flight_id;
  public Long get_flight_id() {
    return flight_id;
  }
  public void set_flight_id(Long flight_id) {
    this.flight_id = flight_id;
  }
  public flights_2024_raw with_flight_id(Long flight_id) {
    this.flight_id = flight_id;
    return this;
  }
  private Integer year;
  public Integer get_year() {
    return year;
  }
  public void set_year(Integer year) {
    this.year = year;
  }
  public flights_2024_raw with_year(Integer year) {
    this.year = year;
    return this;
  }
  private Integer month;
  public Integer get_month() {
    return month;
  }
  public void set_month(Integer month) {
    this.month = month;
  }
  public flights_2024_raw with_month(Integer month) {
    this.month = month;
    return this;
  }
  private Integer day_of_month;
  public Integer get_day_of_month() {
    return day_of_month;
  }
  public void set_day_of_month(Integer day_of_month) {
    this.day_of_month = day_of_month;
  }
  public flights_2024_raw with_day_of_month(Integer day_of_month) {
    this.day_of_month = day_of_month;
    return this;
  }
  private Integer day_of_week;
  public Integer get_day_of_week() {
    return day_of_week;
  }
  public void set_day_of_week(Integer day_of_week) {
    this.day_of_week = day_of_week;
  }
  public flights_2024_raw with_day_of_week(Integer day_of_week) {
    this.day_of_week = day_of_week;
    return this;
  }
  private java.sql.Date fl_date;
  public java.sql.Date get_fl_date() {
    return fl_date;
  }
  public void set_fl_date(java.sql.Date fl_date) {
    this.fl_date = fl_date;
  }
  public flights_2024_raw with_fl_date(java.sql.Date fl_date) {
    this.fl_date = fl_date;
    return this;
  }
  private String op_unique_carrier;
  public String get_op_unique_carrier() {
    return op_unique_carrier;
  }
  public void set_op_unique_carrier(String op_unique_carrier) {
    this.op_unique_carrier = op_unique_carrier;
  }
  public flights_2024_raw with_op_unique_carrier(String op_unique_carrier) {
    this.op_unique_carrier = op_unique_carrier;
    return this;
  }
  private java.math.BigDecimal op_carrier_fl_num;
  public java.math.BigDecimal get_op_carrier_fl_num() {
    return op_carrier_fl_num;
  }
  public void set_op_carrier_fl_num(java.math.BigDecimal op_carrier_fl_num) {
    this.op_carrier_fl_num = op_carrier_fl_num;
  }
  public flights_2024_raw with_op_carrier_fl_num(java.math.BigDecimal op_carrier_fl_num) {
    this.op_carrier_fl_num = op_carrier_fl_num;
    return this;
  }
  private String origin;
  public String get_origin() {
    return origin;
  }
  public void set_origin(String origin) {
    this.origin = origin;
  }
  public flights_2024_raw with_origin(String origin) {
    this.origin = origin;
    return this;
  }
  private String origin_city_name;
  public String get_origin_city_name() {
    return origin_city_name;
  }
  public void set_origin_city_name(String origin_city_name) {
    this.origin_city_name = origin_city_name;
  }
  public flights_2024_raw with_origin_city_name(String origin_city_name) {
    this.origin_city_name = origin_city_name;
    return this;
  }
  private String origin_state_nm;
  public String get_origin_state_nm() {
    return origin_state_nm;
  }
  public void set_origin_state_nm(String origin_state_nm) {
    this.origin_state_nm = origin_state_nm;
  }
  public flights_2024_raw with_origin_state_nm(String origin_state_nm) {
    this.origin_state_nm = origin_state_nm;
    return this;
  }
  private String dest;
  public String get_dest() {
    return dest;
  }
  public void set_dest(String dest) {
    this.dest = dest;
  }
  public flights_2024_raw with_dest(String dest) {
    this.dest = dest;
    return this;
  }
  private String dest_city_name;
  public String get_dest_city_name() {
    return dest_city_name;
  }
  public void set_dest_city_name(String dest_city_name) {
    this.dest_city_name = dest_city_name;
  }
  public flights_2024_raw with_dest_city_name(String dest_city_name) {
    this.dest_city_name = dest_city_name;
    return this;
  }
  private String dest_state_nm;
  public String get_dest_state_nm() {
    return dest_state_nm;
  }
  public void set_dest_state_nm(String dest_state_nm) {
    this.dest_state_nm = dest_state_nm;
  }
  public flights_2024_raw with_dest_state_nm(String dest_state_nm) {
    this.dest_state_nm = dest_state_nm;
    return this;
  }
  private Integer crs_dep_time;
  public Integer get_crs_dep_time() {
    return crs_dep_time;
  }
  public void set_crs_dep_time(Integer crs_dep_time) {
    this.crs_dep_time = crs_dep_time;
  }
  public flights_2024_raw with_crs_dep_time(Integer crs_dep_time) {
    this.crs_dep_time = crs_dep_time;
    return this;
  }
  private java.math.BigDecimal dep_time;
  public java.math.BigDecimal get_dep_time() {
    return dep_time;
  }
  public void set_dep_time(java.math.BigDecimal dep_time) {
    this.dep_time = dep_time;
  }
  public flights_2024_raw with_dep_time(java.math.BigDecimal dep_time) {
    this.dep_time = dep_time;
    return this;
  }
  private java.math.BigDecimal dep_delay;
  public java.math.BigDecimal get_dep_delay() {
    return dep_delay;
  }
  public void set_dep_delay(java.math.BigDecimal dep_delay) {
    this.dep_delay = dep_delay;
  }
  public flights_2024_raw with_dep_delay(java.math.BigDecimal dep_delay) {
    this.dep_delay = dep_delay;
    return this;
  }
  private java.math.BigDecimal taxi_out;
  public java.math.BigDecimal get_taxi_out() {
    return taxi_out;
  }
  public void set_taxi_out(java.math.BigDecimal taxi_out) {
    this.taxi_out = taxi_out;
  }
  public flights_2024_raw with_taxi_out(java.math.BigDecimal taxi_out) {
    this.taxi_out = taxi_out;
    return this;
  }
  private java.math.BigDecimal wheels_off;
  public java.math.BigDecimal get_wheels_off() {
    return wheels_off;
  }
  public void set_wheels_off(java.math.BigDecimal wheels_off) {
    this.wheels_off = wheels_off;
  }
  public flights_2024_raw with_wheels_off(java.math.BigDecimal wheels_off) {
    this.wheels_off = wheels_off;
    return this;
  }
  private java.math.BigDecimal wheels_on;
  public java.math.BigDecimal get_wheels_on() {
    return wheels_on;
  }
  public void set_wheels_on(java.math.BigDecimal wheels_on) {
    this.wheels_on = wheels_on;
  }
  public flights_2024_raw with_wheels_on(java.math.BigDecimal wheels_on) {
    this.wheels_on = wheels_on;
    return this;
  }
  private java.math.BigDecimal taxi_in;
  public java.math.BigDecimal get_taxi_in() {
    return taxi_in;
  }
  public void set_taxi_in(java.math.BigDecimal taxi_in) {
    this.taxi_in = taxi_in;
  }
  public flights_2024_raw with_taxi_in(java.math.BigDecimal taxi_in) {
    this.taxi_in = taxi_in;
    return this;
  }
  private Integer crs_arr_time;
  public Integer get_crs_arr_time() {
    return crs_arr_time;
  }
  public void set_crs_arr_time(Integer crs_arr_time) {
    this.crs_arr_time = crs_arr_time;
  }
  public flights_2024_raw with_crs_arr_time(Integer crs_arr_time) {
    this.crs_arr_time = crs_arr_time;
    return this;
  }
  private java.math.BigDecimal arr_time;
  public java.math.BigDecimal get_arr_time() {
    return arr_time;
  }
  public void set_arr_time(java.math.BigDecimal arr_time) {
    this.arr_time = arr_time;
  }
  public flights_2024_raw with_arr_time(java.math.BigDecimal arr_time) {
    this.arr_time = arr_time;
    return this;
  }
  private java.math.BigDecimal arr_delay;
  public java.math.BigDecimal get_arr_delay() {
    return arr_delay;
  }
  public void set_arr_delay(java.math.BigDecimal arr_delay) {
    this.arr_delay = arr_delay;
  }
  public flights_2024_raw with_arr_delay(java.math.BigDecimal arr_delay) {
    this.arr_delay = arr_delay;
    return this;
  }
  private Integer cancelled;
  public Integer get_cancelled() {
    return cancelled;
  }
  public void set_cancelled(Integer cancelled) {
    this.cancelled = cancelled;
  }
  public flights_2024_raw with_cancelled(Integer cancelled) {
    this.cancelled = cancelled;
    return this;
  }
  private String cancellation_code;
  public String get_cancellation_code() {
    return cancellation_code;
  }
  public void set_cancellation_code(String cancellation_code) {
    this.cancellation_code = cancellation_code;
  }
  public flights_2024_raw with_cancellation_code(String cancellation_code) {
    this.cancellation_code = cancellation_code;
    return this;
  }
  private Integer diverted;
  public Integer get_diverted() {
    return diverted;
  }
  public void set_diverted(Integer diverted) {
    this.diverted = diverted;
  }
  public flights_2024_raw with_diverted(Integer diverted) {
    this.diverted = diverted;
    return this;
  }
  private java.math.BigDecimal crs_elapsed_time;
  public java.math.BigDecimal get_crs_elapsed_time() {
    return crs_elapsed_time;
  }
  public void set_crs_elapsed_time(java.math.BigDecimal crs_elapsed_time) {
    this.crs_elapsed_time = crs_elapsed_time;
  }
  public flights_2024_raw with_crs_elapsed_time(java.math.BigDecimal crs_elapsed_time) {
    this.crs_elapsed_time = crs_elapsed_time;
    return this;
  }
  private java.math.BigDecimal actual_elapsed_time;
  public java.math.BigDecimal get_actual_elapsed_time() {
    return actual_elapsed_time;
  }
  public void set_actual_elapsed_time(java.math.BigDecimal actual_elapsed_time) {
    this.actual_elapsed_time = actual_elapsed_time;
  }
  public flights_2024_raw with_actual_elapsed_time(java.math.BigDecimal actual_elapsed_time) {
    this.actual_elapsed_time = actual_elapsed_time;
    return this;
  }
  private java.math.BigDecimal air_time;
  public java.math.BigDecimal get_air_time() {
    return air_time;
  }
  public void set_air_time(java.math.BigDecimal air_time) {
    this.air_time = air_time;
  }
  public flights_2024_raw with_air_time(java.math.BigDecimal air_time) {
    this.air_time = air_time;
    return this;
  }
  private java.math.BigDecimal distance;
  public java.math.BigDecimal get_distance() {
    return distance;
  }
  public void set_distance(java.math.BigDecimal distance) {
    this.distance = distance;
  }
  public flights_2024_raw with_distance(java.math.BigDecimal distance) {
    this.distance = distance;
    return this;
  }
  private Integer carrier_delay;
  public Integer get_carrier_delay() {
    return carrier_delay;
  }
  public void set_carrier_delay(Integer carrier_delay) {
    this.carrier_delay = carrier_delay;
  }
  public flights_2024_raw with_carrier_delay(Integer carrier_delay) {
    this.carrier_delay = carrier_delay;
    return this;
  }
  private Integer weather_delay;
  public Integer get_weather_delay() {
    return weather_delay;
  }
  public void set_weather_delay(Integer weather_delay) {
    this.weather_delay = weather_delay;
  }
  public flights_2024_raw with_weather_delay(Integer weather_delay) {
    this.weather_delay = weather_delay;
    return this;
  }
  private Integer nas_delay;
  public Integer get_nas_delay() {
    return nas_delay;
  }
  public void set_nas_delay(Integer nas_delay) {
    this.nas_delay = nas_delay;
  }
  public flights_2024_raw with_nas_delay(Integer nas_delay) {
    this.nas_delay = nas_delay;
    return this;
  }
  private Integer security_delay;
  public Integer get_security_delay() {
    return security_delay;
  }
  public void set_security_delay(Integer security_delay) {
    this.security_delay = security_delay;
  }
  public flights_2024_raw with_security_delay(Integer security_delay) {
    this.security_delay = security_delay;
    return this;
  }
  private Integer late_aircraft_delay;
  public Integer get_late_aircraft_delay() {
    return late_aircraft_delay;
  }
  public void set_late_aircraft_delay(Integer late_aircraft_delay) {
    this.late_aircraft_delay = late_aircraft_delay;
  }
  public flights_2024_raw with_late_aircraft_delay(Integer late_aircraft_delay) {
    this.late_aircraft_delay = late_aircraft_delay;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof flights_2024_raw)) {
      return false;
    }
    flights_2024_raw that = (flights_2024_raw) o;
    boolean equal = true;
    equal = equal && (this.flight_id == null ? that.flight_id == null : this.flight_id.equals(that.flight_id));
    equal = equal && (this.year == null ? that.year == null : this.year.equals(that.year));
    equal = equal && (this.month == null ? that.month == null : this.month.equals(that.month));
    equal = equal && (this.day_of_month == null ? that.day_of_month == null : this.day_of_month.equals(that.day_of_month));
    equal = equal && (this.day_of_week == null ? that.day_of_week == null : this.day_of_week.equals(that.day_of_week));
    equal = equal && (this.fl_date == null ? that.fl_date == null : this.fl_date.equals(that.fl_date));
    equal = equal && (this.op_unique_carrier == null ? that.op_unique_carrier == null : this.op_unique_carrier.equals(that.op_unique_carrier));
    equal = equal && (this.op_carrier_fl_num == null ? that.op_carrier_fl_num == null : this.op_carrier_fl_num.equals(that.op_carrier_fl_num));
    equal = equal && (this.origin == null ? that.origin == null : this.origin.equals(that.origin));
    equal = equal && (this.origin_city_name == null ? that.origin_city_name == null : this.origin_city_name.equals(that.origin_city_name));
    equal = equal && (this.origin_state_nm == null ? that.origin_state_nm == null : this.origin_state_nm.equals(that.origin_state_nm));
    equal = equal && (this.dest == null ? that.dest == null : this.dest.equals(that.dest));
    equal = equal && (this.dest_city_name == null ? that.dest_city_name == null : this.dest_city_name.equals(that.dest_city_name));
    equal = equal && (this.dest_state_nm == null ? that.dest_state_nm == null : this.dest_state_nm.equals(that.dest_state_nm));
    equal = equal && (this.crs_dep_time == null ? that.crs_dep_time == null : this.crs_dep_time.equals(that.crs_dep_time));
    equal = equal && (this.dep_time == null ? that.dep_time == null : this.dep_time.equals(that.dep_time));
    equal = equal && (this.dep_delay == null ? that.dep_delay == null : this.dep_delay.equals(that.dep_delay));
    equal = equal && (this.taxi_out == null ? that.taxi_out == null : this.taxi_out.equals(that.taxi_out));
    equal = equal && (this.wheels_off == null ? that.wheels_off == null : this.wheels_off.equals(that.wheels_off));
    equal = equal && (this.wheels_on == null ? that.wheels_on == null : this.wheels_on.equals(that.wheels_on));
    equal = equal && (this.taxi_in == null ? that.taxi_in == null : this.taxi_in.equals(that.taxi_in));
    equal = equal && (this.crs_arr_time == null ? that.crs_arr_time == null : this.crs_arr_time.equals(that.crs_arr_time));
    equal = equal && (this.arr_time == null ? that.arr_time == null : this.arr_time.equals(that.arr_time));
    equal = equal && (this.arr_delay == null ? that.arr_delay == null : this.arr_delay.equals(that.arr_delay));
    equal = equal && (this.cancelled == null ? that.cancelled == null : this.cancelled.equals(that.cancelled));
    equal = equal && (this.cancellation_code == null ? that.cancellation_code == null : this.cancellation_code.equals(that.cancellation_code));
    equal = equal && (this.diverted == null ? that.diverted == null : this.diverted.equals(that.diverted));
    equal = equal && (this.crs_elapsed_time == null ? that.crs_elapsed_time == null : this.crs_elapsed_time.equals(that.crs_elapsed_time));
    equal = equal && (this.actual_elapsed_time == null ? that.actual_elapsed_time == null : this.actual_elapsed_time.equals(that.actual_elapsed_time));
    equal = equal && (this.air_time == null ? that.air_time == null : this.air_time.equals(that.air_time));
    equal = equal && (this.distance == null ? that.distance == null : this.distance.equals(that.distance));
    equal = equal && (this.carrier_delay == null ? that.carrier_delay == null : this.carrier_delay.equals(that.carrier_delay));
    equal = equal && (this.weather_delay == null ? that.weather_delay == null : this.weather_delay.equals(that.weather_delay));
    equal = equal && (this.nas_delay == null ? that.nas_delay == null : this.nas_delay.equals(that.nas_delay));
    equal = equal && (this.security_delay == null ? that.security_delay == null : this.security_delay.equals(that.security_delay));
    equal = equal && (this.late_aircraft_delay == null ? that.late_aircraft_delay == null : this.late_aircraft_delay.equals(that.late_aircraft_delay));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof flights_2024_raw)) {
      return false;
    }
    flights_2024_raw that = (flights_2024_raw) o;
    boolean equal = true;
    equal = equal && (this.flight_id == null ? that.flight_id == null : this.flight_id.equals(that.flight_id));
    equal = equal && (this.year == null ? that.year == null : this.year.equals(that.year));
    equal = equal && (this.month == null ? that.month == null : this.month.equals(that.month));
    equal = equal && (this.day_of_month == null ? that.day_of_month == null : this.day_of_month.equals(that.day_of_month));
    equal = equal && (this.day_of_week == null ? that.day_of_week == null : this.day_of_week.equals(that.day_of_week));
    equal = equal && (this.fl_date == null ? that.fl_date == null : this.fl_date.equals(that.fl_date));
    equal = equal && (this.op_unique_carrier == null ? that.op_unique_carrier == null : this.op_unique_carrier.equals(that.op_unique_carrier));
    equal = equal && (this.op_carrier_fl_num == null ? that.op_carrier_fl_num == null : this.op_carrier_fl_num.equals(that.op_carrier_fl_num));
    equal = equal && (this.origin == null ? that.origin == null : this.origin.equals(that.origin));
    equal = equal && (this.origin_city_name == null ? that.origin_city_name == null : this.origin_city_name.equals(that.origin_city_name));
    equal = equal && (this.origin_state_nm == null ? that.origin_state_nm == null : this.origin_state_nm.equals(that.origin_state_nm));
    equal = equal && (this.dest == null ? that.dest == null : this.dest.equals(that.dest));
    equal = equal && (this.dest_city_name == null ? that.dest_city_name == null : this.dest_city_name.equals(that.dest_city_name));
    equal = equal && (this.dest_state_nm == null ? that.dest_state_nm == null : this.dest_state_nm.equals(that.dest_state_nm));
    equal = equal && (this.crs_dep_time == null ? that.crs_dep_time == null : this.crs_dep_time.equals(that.crs_dep_time));
    equal = equal && (this.dep_time == null ? that.dep_time == null : this.dep_time.equals(that.dep_time));
    equal = equal && (this.dep_delay == null ? that.dep_delay == null : this.dep_delay.equals(that.dep_delay));
    equal = equal && (this.taxi_out == null ? that.taxi_out == null : this.taxi_out.equals(that.taxi_out));
    equal = equal && (this.wheels_off == null ? that.wheels_off == null : this.wheels_off.equals(that.wheels_off));
    equal = equal && (this.wheels_on == null ? that.wheels_on == null : this.wheels_on.equals(that.wheels_on));
    equal = equal && (this.taxi_in == null ? that.taxi_in == null : this.taxi_in.equals(that.taxi_in));
    equal = equal && (this.crs_arr_time == null ? that.crs_arr_time == null : this.crs_arr_time.equals(that.crs_arr_time));
    equal = equal && (this.arr_time == null ? that.arr_time == null : this.arr_time.equals(that.arr_time));
    equal = equal && (this.arr_delay == null ? that.arr_delay == null : this.arr_delay.equals(that.arr_delay));
    equal = equal && (this.cancelled == null ? that.cancelled == null : this.cancelled.equals(that.cancelled));
    equal = equal && (this.cancellation_code == null ? that.cancellation_code == null : this.cancellation_code.equals(that.cancellation_code));
    equal = equal && (this.diverted == null ? that.diverted == null : this.diverted.equals(that.diverted));
    equal = equal && (this.crs_elapsed_time == null ? that.crs_elapsed_time == null : this.crs_elapsed_time.equals(that.crs_elapsed_time));
    equal = equal && (this.actual_elapsed_time == null ? that.actual_elapsed_time == null : this.actual_elapsed_time.equals(that.actual_elapsed_time));
    equal = equal && (this.air_time == null ? that.air_time == null : this.air_time.equals(that.air_time));
    equal = equal && (this.distance == null ? that.distance == null : this.distance.equals(that.distance));
    equal = equal && (this.carrier_delay == null ? that.carrier_delay == null : this.carrier_delay.equals(that.carrier_delay));
    equal = equal && (this.weather_delay == null ? that.weather_delay == null : this.weather_delay.equals(that.weather_delay));
    equal = equal && (this.nas_delay == null ? that.nas_delay == null : this.nas_delay.equals(that.nas_delay));
    equal = equal && (this.security_delay == null ? that.security_delay == null : this.security_delay.equals(that.security_delay));
    equal = equal && (this.late_aircraft_delay == null ? that.late_aircraft_delay == null : this.late_aircraft_delay.equals(that.late_aircraft_delay));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.flight_id = JdbcWritableBridge.readLong(1, __dbResults);
    this.year = JdbcWritableBridge.readInteger(2, __dbResults);
    this.month = JdbcWritableBridge.readInteger(3, __dbResults);
    this.day_of_month = JdbcWritableBridge.readInteger(4, __dbResults);
    this.day_of_week = JdbcWritableBridge.readInteger(5, __dbResults);
    this.fl_date = JdbcWritableBridge.readDate(6, __dbResults);
    this.op_unique_carrier = JdbcWritableBridge.readString(7, __dbResults);
    this.op_carrier_fl_num = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.origin = JdbcWritableBridge.readString(9, __dbResults);
    this.origin_city_name = JdbcWritableBridge.readString(10, __dbResults);
    this.origin_state_nm = JdbcWritableBridge.readString(11, __dbResults);
    this.dest = JdbcWritableBridge.readString(12, __dbResults);
    this.dest_city_name = JdbcWritableBridge.readString(13, __dbResults);
    this.dest_state_nm = JdbcWritableBridge.readString(14, __dbResults);
    this.crs_dep_time = JdbcWritableBridge.readInteger(15, __dbResults);
    this.dep_time = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.dep_delay = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.taxi_out = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.wheels_off = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.wheels_on = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.taxi_in = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.crs_arr_time = JdbcWritableBridge.readInteger(22, __dbResults);
    this.arr_time = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.arr_delay = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.cancelled = JdbcWritableBridge.readInteger(25, __dbResults);
    this.cancellation_code = JdbcWritableBridge.readString(26, __dbResults);
    this.diverted = JdbcWritableBridge.readInteger(27, __dbResults);
    this.crs_elapsed_time = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.actual_elapsed_time = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.air_time = JdbcWritableBridge.readBigDecimal(30, __dbResults);
    this.distance = JdbcWritableBridge.readBigDecimal(31, __dbResults);
    this.carrier_delay = JdbcWritableBridge.readInteger(32, __dbResults);
    this.weather_delay = JdbcWritableBridge.readInteger(33, __dbResults);
    this.nas_delay = JdbcWritableBridge.readInteger(34, __dbResults);
    this.security_delay = JdbcWritableBridge.readInteger(35, __dbResults);
    this.late_aircraft_delay = JdbcWritableBridge.readInteger(36, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.flight_id = JdbcWritableBridge.readLong(1, __dbResults);
    this.year = JdbcWritableBridge.readInteger(2, __dbResults);
    this.month = JdbcWritableBridge.readInteger(3, __dbResults);
    this.day_of_month = JdbcWritableBridge.readInteger(4, __dbResults);
    this.day_of_week = JdbcWritableBridge.readInteger(5, __dbResults);
    this.fl_date = JdbcWritableBridge.readDate(6, __dbResults);
    this.op_unique_carrier = JdbcWritableBridge.readString(7, __dbResults);
    this.op_carrier_fl_num = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.origin = JdbcWritableBridge.readString(9, __dbResults);
    this.origin_city_name = JdbcWritableBridge.readString(10, __dbResults);
    this.origin_state_nm = JdbcWritableBridge.readString(11, __dbResults);
    this.dest = JdbcWritableBridge.readString(12, __dbResults);
    this.dest_city_name = JdbcWritableBridge.readString(13, __dbResults);
    this.dest_state_nm = JdbcWritableBridge.readString(14, __dbResults);
    this.crs_dep_time = JdbcWritableBridge.readInteger(15, __dbResults);
    this.dep_time = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.dep_delay = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.taxi_out = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.wheels_off = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.wheels_on = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.taxi_in = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.crs_arr_time = JdbcWritableBridge.readInteger(22, __dbResults);
    this.arr_time = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.arr_delay = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.cancelled = JdbcWritableBridge.readInteger(25, __dbResults);
    this.cancellation_code = JdbcWritableBridge.readString(26, __dbResults);
    this.diverted = JdbcWritableBridge.readInteger(27, __dbResults);
    this.crs_elapsed_time = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.actual_elapsed_time = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.air_time = JdbcWritableBridge.readBigDecimal(30, __dbResults);
    this.distance = JdbcWritableBridge.readBigDecimal(31, __dbResults);
    this.carrier_delay = JdbcWritableBridge.readInteger(32, __dbResults);
    this.weather_delay = JdbcWritableBridge.readInteger(33, __dbResults);
    this.nas_delay = JdbcWritableBridge.readInteger(34, __dbResults);
    this.security_delay = JdbcWritableBridge.readInteger(35, __dbResults);
    this.late_aircraft_delay = JdbcWritableBridge.readInteger(36, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(flight_id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(year, 2 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(month, 3 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(day_of_month, 4 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(day_of_week, 5 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeDate(fl_date, 6 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(op_unique_carrier, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(op_carrier_fl_num, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(origin, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(origin_city_name, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(origin_state_nm, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dest, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dest_city_name, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dest_state_nm, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(crs_dep_time, 15 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(dep_time, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(dep_delay, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(taxi_out, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(wheels_off, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(wheels_on, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(taxi_in, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeInteger(crs_arr_time, 22 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(arr_time, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(arr_delay, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeInteger(cancelled, 25 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeString(cancellation_code, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(diverted, 27 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(crs_elapsed_time, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(actual_elapsed_time, 29 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(air_time, 30 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(distance, 31 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeInteger(carrier_delay, 32 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(weather_delay, 33 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(nas_delay, 34 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(security_delay, 35 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(late_aircraft_delay, 36 + __off, 4, __dbStmt);
    return 36;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(flight_id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(year, 2 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(month, 3 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(day_of_month, 4 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(day_of_week, 5 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeDate(fl_date, 6 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(op_unique_carrier, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(op_carrier_fl_num, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(origin, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(origin_city_name, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(origin_state_nm, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dest, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dest_city_name, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dest_state_nm, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(crs_dep_time, 15 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(dep_time, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(dep_delay, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(taxi_out, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(wheels_off, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(wheels_on, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(taxi_in, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeInteger(crs_arr_time, 22 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(arr_time, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(arr_delay, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeInteger(cancelled, 25 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeString(cancellation_code, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(diverted, 27 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(crs_elapsed_time, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(actual_elapsed_time, 29 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(air_time, 30 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(distance, 31 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeInteger(carrier_delay, 32 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(weather_delay, 33 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(nas_delay, 34 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(security_delay, 35 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(late_aircraft_delay, 36 + __off, 4, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.flight_id = null;
    } else {
    this.flight_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.year = null;
    } else {
    this.year = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.month = null;
    } else {
    this.month = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.day_of_month = null;
    } else {
    this.day_of_month = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.day_of_week = null;
    } else {
    this.day_of_week = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.fl_date = null;
    } else {
    this.fl_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.op_unique_carrier = null;
    } else {
    this.op_unique_carrier = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.op_carrier_fl_num = null;
    } else {
    this.op_carrier_fl_num = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.origin = null;
    } else {
    this.origin = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.origin_city_name = null;
    } else {
    this.origin_city_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.origin_state_nm = null;
    } else {
    this.origin_state_nm = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.dest = null;
    } else {
    this.dest = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.dest_city_name = null;
    } else {
    this.dest_city_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.dest_state_nm = null;
    } else {
    this.dest_state_nm = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.crs_dep_time = null;
    } else {
    this.crs_dep_time = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.dep_time = null;
    } else {
    this.dep_time = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.dep_delay = null;
    } else {
    this.dep_delay = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.taxi_out = null;
    } else {
    this.taxi_out = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wheels_off = null;
    } else {
    this.wheels_off = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wheels_on = null;
    } else {
    this.wheels_on = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.taxi_in = null;
    } else {
    this.taxi_in = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.crs_arr_time = null;
    } else {
    this.crs_arr_time = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.arr_time = null;
    } else {
    this.arr_time = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.arr_delay = null;
    } else {
    this.arr_delay = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cancelled = null;
    } else {
    this.cancelled = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.cancellation_code = null;
    } else {
    this.cancellation_code = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.diverted = null;
    } else {
    this.diverted = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.crs_elapsed_time = null;
    } else {
    this.crs_elapsed_time = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.actual_elapsed_time = null;
    } else {
    this.actual_elapsed_time = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.air_time = null;
    } else {
    this.air_time = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.distance = null;
    } else {
    this.distance = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.carrier_delay = null;
    } else {
    this.carrier_delay = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.weather_delay = null;
    } else {
    this.weather_delay = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.nas_delay = null;
    } else {
    this.nas_delay = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.security_delay = null;
    } else {
    this.security_delay = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.late_aircraft_delay = null;
    } else {
    this.late_aircraft_delay = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.flight_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.flight_id);
    }
    if (null == this.year) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.year);
    }
    if (null == this.month) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.month);
    }
    if (null == this.day_of_month) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.day_of_month);
    }
    if (null == this.day_of_week) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.day_of_week);
    }
    if (null == this.fl_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.fl_date.getTime());
    }
    if (null == this.op_unique_carrier) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, op_unique_carrier);
    }
    if (null == this.op_carrier_fl_num) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.op_carrier_fl_num, __dataOut);
    }
    if (null == this.origin) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, origin);
    }
    if (null == this.origin_city_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, origin_city_name);
    }
    if (null == this.origin_state_nm) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, origin_state_nm);
    }
    if (null == this.dest) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dest);
    }
    if (null == this.dest_city_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dest_city_name);
    }
    if (null == this.dest_state_nm) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dest_state_nm);
    }
    if (null == this.crs_dep_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.crs_dep_time);
    }
    if (null == this.dep_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.dep_time, __dataOut);
    }
    if (null == this.dep_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.dep_delay, __dataOut);
    }
    if (null == this.taxi_out) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.taxi_out, __dataOut);
    }
    if (null == this.wheels_off) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.wheels_off, __dataOut);
    }
    if (null == this.wheels_on) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.wheels_on, __dataOut);
    }
    if (null == this.taxi_in) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.taxi_in, __dataOut);
    }
    if (null == this.crs_arr_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.crs_arr_time);
    }
    if (null == this.arr_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.arr_time, __dataOut);
    }
    if (null == this.arr_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.arr_delay, __dataOut);
    }
    if (null == this.cancelled) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.cancelled);
    }
    if (null == this.cancellation_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cancellation_code);
    }
    if (null == this.diverted) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.diverted);
    }
    if (null == this.crs_elapsed_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.crs_elapsed_time, __dataOut);
    }
    if (null == this.actual_elapsed_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.actual_elapsed_time, __dataOut);
    }
    if (null == this.air_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.air_time, __dataOut);
    }
    if (null == this.distance) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.distance, __dataOut);
    }
    if (null == this.carrier_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.carrier_delay);
    }
    if (null == this.weather_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.weather_delay);
    }
    if (null == this.nas_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.nas_delay);
    }
    if (null == this.security_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.security_delay);
    }
    if (null == this.late_aircraft_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.late_aircraft_delay);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.flight_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.flight_id);
    }
    if (null == this.year) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.year);
    }
    if (null == this.month) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.month);
    }
    if (null == this.day_of_month) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.day_of_month);
    }
    if (null == this.day_of_week) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.day_of_week);
    }
    if (null == this.fl_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.fl_date.getTime());
    }
    if (null == this.op_unique_carrier) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, op_unique_carrier);
    }
    if (null == this.op_carrier_fl_num) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.op_carrier_fl_num, __dataOut);
    }
    if (null == this.origin) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, origin);
    }
    if (null == this.origin_city_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, origin_city_name);
    }
    if (null == this.origin_state_nm) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, origin_state_nm);
    }
    if (null == this.dest) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dest);
    }
    if (null == this.dest_city_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dest_city_name);
    }
    if (null == this.dest_state_nm) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dest_state_nm);
    }
    if (null == this.crs_dep_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.crs_dep_time);
    }
    if (null == this.dep_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.dep_time, __dataOut);
    }
    if (null == this.dep_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.dep_delay, __dataOut);
    }
    if (null == this.taxi_out) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.taxi_out, __dataOut);
    }
    if (null == this.wheels_off) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.wheels_off, __dataOut);
    }
    if (null == this.wheels_on) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.wheels_on, __dataOut);
    }
    if (null == this.taxi_in) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.taxi_in, __dataOut);
    }
    if (null == this.crs_arr_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.crs_arr_time);
    }
    if (null == this.arr_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.arr_time, __dataOut);
    }
    if (null == this.arr_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.arr_delay, __dataOut);
    }
    if (null == this.cancelled) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.cancelled);
    }
    if (null == this.cancellation_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cancellation_code);
    }
    if (null == this.diverted) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.diverted);
    }
    if (null == this.crs_elapsed_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.crs_elapsed_time, __dataOut);
    }
    if (null == this.actual_elapsed_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.actual_elapsed_time, __dataOut);
    }
    if (null == this.air_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.air_time, __dataOut);
    }
    if (null == this.distance) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.distance, __dataOut);
    }
    if (null == this.carrier_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.carrier_delay);
    }
    if (null == this.weather_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.weather_delay);
    }
    if (null == this.nas_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.nas_delay);
    }
    if (null == this.security_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.security_delay);
    }
    if (null == this.late_aircraft_delay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.late_aircraft_delay);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(flight_id==null?"null":"" + flight_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(year==null?"null":"" + year, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(month==null?"null":"" + month, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_of_month==null?"null":"" + day_of_month, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_of_week==null?"null":"" + day_of_week, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fl_date==null?"null":"" + fl_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(op_unique_carrier==null?"null":op_unique_carrier, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(op_carrier_fl_num==null?"null":op_carrier_fl_num.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(origin==null?"null":origin, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(origin_city_name==null?"null":origin_city_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(origin_state_nm==null?"null":origin_state_nm, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dest==null?"null":dest, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dest_city_name==null?"null":dest_city_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dest_state_nm==null?"null":dest_state_nm, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crs_dep_time==null?"null":"" + crs_dep_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dep_time==null?"null":dep_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dep_delay==null?"null":dep_delay.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(taxi_out==null?"null":taxi_out.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wheels_off==null?"null":wheels_off.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wheels_on==null?"null":wheels_on.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(taxi_in==null?"null":taxi_in.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crs_arr_time==null?"null":"" + crs_arr_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(arr_time==null?"null":arr_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(arr_delay==null?"null":arr_delay.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cancelled==null?"null":"" + cancelled, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cancellation_code==null?"null":cancellation_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(diverted==null?"null":"" + diverted, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crs_elapsed_time==null?"null":crs_elapsed_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(actual_elapsed_time==null?"null":actual_elapsed_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(air_time==null?"null":air_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(distance==null?"null":distance.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(carrier_delay==null?"null":"" + carrier_delay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_delay==null?"null":"" + weather_delay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(nas_delay==null?"null":"" + nas_delay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(security_delay==null?"null":"" + security_delay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(late_aircraft_delay==null?"null":"" + late_aircraft_delay, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(flight_id==null?"null":"" + flight_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(year==null?"null":"" + year, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(month==null?"null":"" + month, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_of_month==null?"null":"" + day_of_month, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_of_week==null?"null":"" + day_of_week, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fl_date==null?"null":"" + fl_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(op_unique_carrier==null?"null":op_unique_carrier, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(op_carrier_fl_num==null?"null":op_carrier_fl_num.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(origin==null?"null":origin, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(origin_city_name==null?"null":origin_city_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(origin_state_nm==null?"null":origin_state_nm, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dest==null?"null":dest, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dest_city_name==null?"null":dest_city_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dest_state_nm==null?"null":dest_state_nm, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crs_dep_time==null?"null":"" + crs_dep_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dep_time==null?"null":dep_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dep_delay==null?"null":dep_delay.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(taxi_out==null?"null":taxi_out.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wheels_off==null?"null":wheels_off.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wheels_on==null?"null":wheels_on.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(taxi_in==null?"null":taxi_in.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crs_arr_time==null?"null":"" + crs_arr_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(arr_time==null?"null":arr_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(arr_delay==null?"null":arr_delay.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cancelled==null?"null":"" + cancelled, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cancellation_code==null?"null":cancellation_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(diverted==null?"null":"" + diverted, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crs_elapsed_time==null?"null":crs_elapsed_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(actual_elapsed_time==null?"null":actual_elapsed_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(air_time==null?"null":air_time.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(distance==null?"null":distance.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(carrier_delay==null?"null":"" + carrier_delay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_delay==null?"null":"" + weather_delay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(nas_delay==null?"null":"" + nas_delay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(security_delay==null?"null":"" + security_delay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(late_aircraft_delay==null?"null":"" + late_aircraft_delay, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.flight_id = null; } else {
      this.flight_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.year = null; } else {
      this.year = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.month = null; } else {
      this.month = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.day_of_month = null; } else {
      this.day_of_month = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.day_of_week = null; } else {
      this.day_of_week = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.fl_date = null; } else {
      this.fl_date = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.op_unique_carrier = null; } else {
      this.op_unique_carrier = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.op_carrier_fl_num = null; } else {
      this.op_carrier_fl_num = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.origin = null; } else {
      this.origin = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.origin_city_name = null; } else {
      this.origin_city_name = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.origin_state_nm = null; } else {
      this.origin_state_nm = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.dest = null; } else {
      this.dest = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.dest_city_name = null; } else {
      this.dest_city_name = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.dest_state_nm = null; } else {
      this.dest_state_nm = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crs_dep_time = null; } else {
      this.crs_dep_time = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dep_time = null; } else {
      this.dep_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dep_delay = null; } else {
      this.dep_delay = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.taxi_out = null; } else {
      this.taxi_out = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wheels_off = null; } else {
      this.wheels_off = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wheels_on = null; } else {
      this.wheels_on = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.taxi_in = null; } else {
      this.taxi_in = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crs_arr_time = null; } else {
      this.crs_arr_time = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.arr_time = null; } else {
      this.arr_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.arr_delay = null; } else {
      this.arr_delay = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.cancelled = null; } else {
      this.cancelled = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.cancellation_code = null; } else {
      this.cancellation_code = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.diverted = null; } else {
      this.diverted = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crs_elapsed_time = null; } else {
      this.crs_elapsed_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.actual_elapsed_time = null; } else {
      this.actual_elapsed_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.air_time = null; } else {
      this.air_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.distance = null; } else {
      this.distance = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.carrier_delay = null; } else {
      this.carrier_delay = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.weather_delay = null; } else {
      this.weather_delay = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.nas_delay = null; } else {
      this.nas_delay = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.security_delay = null; } else {
      this.security_delay = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.late_aircraft_delay = null; } else {
      this.late_aircraft_delay = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.flight_id = null; } else {
      this.flight_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.year = null; } else {
      this.year = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.month = null; } else {
      this.month = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.day_of_month = null; } else {
      this.day_of_month = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.day_of_week = null; } else {
      this.day_of_week = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.fl_date = null; } else {
      this.fl_date = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.op_unique_carrier = null; } else {
      this.op_unique_carrier = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.op_carrier_fl_num = null; } else {
      this.op_carrier_fl_num = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.origin = null; } else {
      this.origin = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.origin_city_name = null; } else {
      this.origin_city_name = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.origin_state_nm = null; } else {
      this.origin_state_nm = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.dest = null; } else {
      this.dest = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.dest_city_name = null; } else {
      this.dest_city_name = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.dest_state_nm = null; } else {
      this.dest_state_nm = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crs_dep_time = null; } else {
      this.crs_dep_time = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dep_time = null; } else {
      this.dep_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dep_delay = null; } else {
      this.dep_delay = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.taxi_out = null; } else {
      this.taxi_out = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wheels_off = null; } else {
      this.wheels_off = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wheels_on = null; } else {
      this.wheels_on = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.taxi_in = null; } else {
      this.taxi_in = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crs_arr_time = null; } else {
      this.crs_arr_time = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.arr_time = null; } else {
      this.arr_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.arr_delay = null; } else {
      this.arr_delay = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.cancelled = null; } else {
      this.cancelled = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.cancellation_code = null; } else {
      this.cancellation_code = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.diverted = null; } else {
      this.diverted = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crs_elapsed_time = null; } else {
      this.crs_elapsed_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.actual_elapsed_time = null; } else {
      this.actual_elapsed_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.air_time = null; } else {
      this.air_time = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.distance = null; } else {
      this.distance = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.carrier_delay = null; } else {
      this.carrier_delay = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.weather_delay = null; } else {
      this.weather_delay = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.nas_delay = null; } else {
      this.nas_delay = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.security_delay = null; } else {
      this.security_delay = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.late_aircraft_delay = null; } else {
      this.late_aircraft_delay = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    flights_2024_raw o = (flights_2024_raw) super.clone();
    o.fl_date = (o.fl_date != null) ? (java.sql.Date) o.fl_date.clone() : null;
    return o;
  }

  public void clone0(flights_2024_raw o) throws CloneNotSupportedException {
    o.fl_date = (o.fl_date != null) ? (java.sql.Date) o.fl_date.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("flight_id", this.flight_id);
    __sqoop$field_map.put("year", this.year);
    __sqoop$field_map.put("month", this.month);
    __sqoop$field_map.put("day_of_month", this.day_of_month);
    __sqoop$field_map.put("day_of_week", this.day_of_week);
    __sqoop$field_map.put("fl_date", this.fl_date);
    __sqoop$field_map.put("op_unique_carrier", this.op_unique_carrier);
    __sqoop$field_map.put("op_carrier_fl_num", this.op_carrier_fl_num);
    __sqoop$field_map.put("origin", this.origin);
    __sqoop$field_map.put("origin_city_name", this.origin_city_name);
    __sqoop$field_map.put("origin_state_nm", this.origin_state_nm);
    __sqoop$field_map.put("dest", this.dest);
    __sqoop$field_map.put("dest_city_name", this.dest_city_name);
    __sqoop$field_map.put("dest_state_nm", this.dest_state_nm);
    __sqoop$field_map.put("crs_dep_time", this.crs_dep_time);
    __sqoop$field_map.put("dep_time", this.dep_time);
    __sqoop$field_map.put("dep_delay", this.dep_delay);
    __sqoop$field_map.put("taxi_out", this.taxi_out);
    __sqoop$field_map.put("wheels_off", this.wheels_off);
    __sqoop$field_map.put("wheels_on", this.wheels_on);
    __sqoop$field_map.put("taxi_in", this.taxi_in);
    __sqoop$field_map.put("crs_arr_time", this.crs_arr_time);
    __sqoop$field_map.put("arr_time", this.arr_time);
    __sqoop$field_map.put("arr_delay", this.arr_delay);
    __sqoop$field_map.put("cancelled", this.cancelled);
    __sqoop$field_map.put("cancellation_code", this.cancellation_code);
    __sqoop$field_map.put("diverted", this.diverted);
    __sqoop$field_map.put("crs_elapsed_time", this.crs_elapsed_time);
    __sqoop$field_map.put("actual_elapsed_time", this.actual_elapsed_time);
    __sqoop$field_map.put("air_time", this.air_time);
    __sqoop$field_map.put("distance", this.distance);
    __sqoop$field_map.put("carrier_delay", this.carrier_delay);
    __sqoop$field_map.put("weather_delay", this.weather_delay);
    __sqoop$field_map.put("nas_delay", this.nas_delay);
    __sqoop$field_map.put("security_delay", this.security_delay);
    __sqoop$field_map.put("late_aircraft_delay", this.late_aircraft_delay);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("flight_id", this.flight_id);
    __sqoop$field_map.put("year", this.year);
    __sqoop$field_map.put("month", this.month);
    __sqoop$field_map.put("day_of_month", this.day_of_month);
    __sqoop$field_map.put("day_of_week", this.day_of_week);
    __sqoop$field_map.put("fl_date", this.fl_date);
    __sqoop$field_map.put("op_unique_carrier", this.op_unique_carrier);
    __sqoop$field_map.put("op_carrier_fl_num", this.op_carrier_fl_num);
    __sqoop$field_map.put("origin", this.origin);
    __sqoop$field_map.put("origin_city_name", this.origin_city_name);
    __sqoop$field_map.put("origin_state_nm", this.origin_state_nm);
    __sqoop$field_map.put("dest", this.dest);
    __sqoop$field_map.put("dest_city_name", this.dest_city_name);
    __sqoop$field_map.put("dest_state_nm", this.dest_state_nm);
    __sqoop$field_map.put("crs_dep_time", this.crs_dep_time);
    __sqoop$field_map.put("dep_time", this.dep_time);
    __sqoop$field_map.put("dep_delay", this.dep_delay);
    __sqoop$field_map.put("taxi_out", this.taxi_out);
    __sqoop$field_map.put("wheels_off", this.wheels_off);
    __sqoop$field_map.put("wheels_on", this.wheels_on);
    __sqoop$field_map.put("taxi_in", this.taxi_in);
    __sqoop$field_map.put("crs_arr_time", this.crs_arr_time);
    __sqoop$field_map.put("arr_time", this.arr_time);
    __sqoop$field_map.put("arr_delay", this.arr_delay);
    __sqoop$field_map.put("cancelled", this.cancelled);
    __sqoop$field_map.put("cancellation_code", this.cancellation_code);
    __sqoop$field_map.put("diverted", this.diverted);
    __sqoop$field_map.put("crs_elapsed_time", this.crs_elapsed_time);
    __sqoop$field_map.put("actual_elapsed_time", this.actual_elapsed_time);
    __sqoop$field_map.put("air_time", this.air_time);
    __sqoop$field_map.put("distance", this.distance);
    __sqoop$field_map.put("carrier_delay", this.carrier_delay);
    __sqoop$field_map.put("weather_delay", this.weather_delay);
    __sqoop$field_map.put("nas_delay", this.nas_delay);
    __sqoop$field_map.put("security_delay", this.security_delay);
    __sqoop$field_map.put("late_aircraft_delay", this.late_aircraft_delay);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
