/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam.game.utils;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Class to hold info about a game play event
 */
@DefaultCoder(AvroCoder.class)
public class PlayEvent {

  @Nullable
  String user;
  @Nullable
  Long timestamp;
  @Nullable
  String eventId;

  public PlayEvent() {
  }

  public PlayEvent(String user, Long timestamp, String eventId) {
    this.user = user;
    this.timestamp = timestamp;
    this.eventId = eventId;
  }

  public String getUser() {
    return this.user;
  }

  public String getKey() {
    return this.user;
  }

  public Long getTimestamp() {
    return this.timestamp;
  }

  public String getEventId() {
    return this.eventId;
  }
}
