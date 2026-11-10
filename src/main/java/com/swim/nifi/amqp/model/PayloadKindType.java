package com.swim.nifi.amqp.model;

/**
 * Represents the distinct types of payloads that can be handled or transmitted.
 *
 * <ul>
 *   <li>{@link #TEXT} - Plain or formatted textual data.
 *   <li>{@link #BINARY} - Raw binary or byte-stream data.
 *   <li>{@link #STRUCTURED} - Highly organized data formats such as JSON, XML, or objects.
 * </ul>
 */
public enum PayloadKindType {
  /** Plain or formatted textual data (e.g., strings, JSON text, logs). */
  TEXT,
  /** Raw binary or byte-stream data. */
  BINARY,
  /**
   * Highly organized data formats structured into objects or schemas (e.g., maps, serialized
   * objects).
   */
  STRUCTURED
}
