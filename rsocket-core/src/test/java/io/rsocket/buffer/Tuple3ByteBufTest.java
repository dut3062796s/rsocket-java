package io.rsocket.buffer;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Ignore;

//@Ignore
public class Tuple3ByteBufTest extends AbstractByteBufTest {

  @Override
  protected ByteBuf newBuffer(int capacity, int maxCapacity) {
    int capacityPerTuple = Math.max(capacity / 3, 1);

    ByteBuf one = wrappedBuffer(new byte[capacityPerTuple]);
    capacity -= capacityPerTuple;
    ByteBuf two = wrappedBuffer(capacity <= 0 ? new byte[0] : new byte[capacityPerTuple]);
    capacity -= capacityPerTuple;
    ByteBuf three = wrappedBuffer(new byte[Math.max(capacity, 0)]);

    return TupleByteBuffs.of(ByteBufAllocator.DEFAULT, one, two, three);
  }
}
