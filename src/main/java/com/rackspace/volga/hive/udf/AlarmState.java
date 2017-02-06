/**
 * Copyright 2014 Rackspace Hosting, Inc.
 */
package com.rackspace.volga.hive.udf;

enum AlarmState {
  UNKNOWN  (-1L, null),
  OK       (0L, "ok"),
  WARNING  (1L, "warning"),
  ERROR    (2L, "error"),
  CRITICAL (3L, "critical"),
  DISABLED (4L, "disabled");
  
  private final Long state;
  private final String text;
  AlarmState(Long state, String text)
  {
    this.state = state;
    this.text = text;
  }

  long asLong() 
  {
    return this.state;
  }
  
  String asString() 
  {
    return this.text;
  }

  static AlarmState makeState(String state_s) {
    AlarmState state = AlarmState.UNKNOWN;
    if(state_s != null) {
      for (AlarmState s : AlarmState.values()) {
        if(state_s.equals(s.asString())) {
          state = s;
          break;
        }
      }
    }
    return state;
  }
  
  static AlarmState makeState(Long state_i) {
    AlarmState state = AlarmState.UNKNOWN;
    if(state_i != null) {
      for (AlarmState s : AlarmState.values()) {
        if(state_i == s.asLong()) {
          state = s;
          break;
        }
      }
    }
    return state;
  }

  
};
    
