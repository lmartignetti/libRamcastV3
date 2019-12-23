package ch.usi.dslab.lel.ramcast;

import com.ibm.disni.util.MemoryUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RamcastMemoryBlockTest {
  ByteBuffer buffer;
  RamcastMemoryBlock memoryBlock;

  @BeforeEach
  public void setUp() {
    buffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_MESSAGE);
    memoryBlock =
        new RamcastMemoryBlock(
            MemoryUtils.getAddress(buffer), 0, RamcastConfig.SIZE_MESSAGE * 5, buffer);
  }

  @Test
  public void testMoveHeadTail() {
    Exception exception =
        assertThrows(IllegalStateException.class, () -> memoryBlock.moveHeadOffset(1));
    String msg = exception.getMessage();
    assertEquals("Head can not pass tail. Current head=0 and tail=0 tail passed head=false", msg);

    memoryBlock.moveTailOffset(2);
    assertEquals(2, memoryBlock.getTailOffset());

    memoryBlock.moveTailOffset(3);
    assertEquals(0, memoryBlock.getTailOffset());

    exception =
        assertThrows(IllegalStateException.class, () -> memoryBlock.moveTailOffset(1));
    msg = exception.getMessage();
    assertEquals("Tail can not pass head. Current head=0 and tail=0 tail passed head=true", msg);
  }
}
