package com.swim.nifi.amqp.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

/** Reads FlowFile content into a contiguous payload buffer for AMQP publication. */
public final class PayloadGenerator {

  /** Constructs a PayloadGenerator processor instance. */
  public PayloadGenerator() {}

  private static void copy(final byte[] bytes, final ByteArrayOutputStream outputStream)
      throws IOException {
    outputStream.write(bytes);
  }

  /**
   * Materializes the current FlowFile payload into a byte array.
   *
   * <p>The implementation uses a streaming read callback to avoid unnecessary intermediate Strings
   * and to keep the conversion logic local to the processor boundary.
   *
   * @param session NiFi process session used to access content
   * @param flowFile source FlowFile
   * @return payload bytes
   */
  public byte[] read(final ProcessSession session, final FlowFile flowFile) {
    final ByteArrayOutputStream outputStream =
        new ByteArrayOutputStream(Math.toIntExact(flowFile.getSize()));
    session.read(flowFile, inputStream -> copy(inputStream.readAllBytes(), outputStream));
    return outputStream.toByteArray();
  }
}
