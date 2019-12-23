package ch.usi.dslab.lel.ramcast.utils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class StringUtils {

  // calculate checksum of buffer from [position - limit]
  public static long calculateCrc32(ByteBuffer buffer) {
    CRC32 checksum = new CRC32();
    checksum.update(buffer);
    return checksum.getValue();
  }
}
