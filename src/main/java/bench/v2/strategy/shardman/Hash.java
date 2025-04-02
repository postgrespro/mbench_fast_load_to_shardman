package bench.v2.strategy.shardman;

/**
 * @author Ivan Frolkov
 */

import com.google.common.primitives.UnsignedInteger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

public class Hash {
	public static final long PartitionSeed = 0x7A5B22367996DCFDL;
	final static int _a = 0;
	final static int _b = 1;
	final static int __b = 0;
	final static int _c = 2;
	final static int __c = 1;

	static UnsignedInteger rot32(UnsignedInteger in, int shift) {
		return UnsignedInteger.fromIntBits(Integer.rotateLeft(in.intValue(), shift));
	}

	static UnsignedInteger[] mix32(UnsignedInteger a, UnsignedInteger b, UnsignedInteger c) {
		a = a.minus(c);
		a = xor(a, rot32(c, 4));
		c = c.plus(b);
		b = b.minus(a);
		b = xor(b, rot32(a, 6));
		a = a.plus(c);
		c = c.minus(b);
		c = xor(c, rot32(b, 8));
		b = b.plus(a);
		a = a.minus(c);
		a = xor(a, rot32(c, 16));
		c = c.plus(b);
		b = b.minus(a);
		b = xor(b, rot32(a, 19));
		a = a.plus(c);
		c = c.minus(b);
		c = xor(c, rot32(b, 4));
		b = b.plus(a);
		return new UnsignedInteger[] { a, b, c };
	}

	static UnsignedInteger[] mix32(UnsignedInteger[] abc) {
		return mix32(abc[_a], abc[_b], abc[_c]);
	}

	static UnsignedInteger[] final32(UnsignedInteger a, UnsignedInteger b, UnsignedInteger c) {
		c = xor(c, b);
		c = c.minus(rot32(b, 14));
		a = xor(a,c);
		a = a.minus(rot32(c, 11));
		b = xor(b,a);
		b = b.minus(rot32(a, 25));
		c = xor(c, b);
		c = c.minus(rot32(b, 16));
		a = xor(a, c);
		a = a.minus(rot32(c, 4));
		b = xor(b, a);
		b = b.minus(rot32(a, 14));
		c = xor(c, b);
		c = c.minus(rot32(b, 24));
		return new UnsignedInteger[] { b, c };
	}

	static UnsignedInteger[] final32(UnsignedInteger[] abc) {
		return final32(abc[_a], abc[_b], abc[_c]);
	}

	static UnsignedInteger[] initABC(int size) {
		int a = 0x9e3779b9 + size + 3923095;
		return new UnsignedInteger[] { UnsignedInteger.fromIntBits(a), UnsignedInteger.fromIntBits(a), UnsignedInteger.fromIntBits(a) };
	}

	public static long hashInt(UnsignedInteger k, long seed) {
		UnsignedInteger[] abc = initABC(4);

		if (seed != 0) {
			abc[_a] = abc[_a].plus(UnsignedInteger.fromIntBits((int) (seed >>> 32)));
			abc[_b] = abc[_b].plus(UnsignedInteger.fromIntBits((int) (seed)));
			abc = mix32(abc);
		}

		abc[_a] = abc[_a].plus(k);
		UnsignedInteger[] bc = final32(abc);

		return bc[__b].longValue() << 32 | bc[__c].longValue();
	}

	public static long hashBytes(byte[] bytes, long seed) {
		UnsignedInteger[] abc = initABC(bytes.length);

		if (seed != 0) {
			abc[_a] = abc[_a].plus(UnsignedInteger.fromIntBits((int) (seed >>> 32)));
			abc[_b] = abc[_b].plus(UnsignedInteger.fromIntBits((int) seed));
			abc = mix32(abc);
		}

		int len = 0;
		while (bytes.length - len >= 12) {
			abc[_a] = abc[_a].plus(UnsignedInteger.fromIntBits(ByteBuffer.wrap(bytes, len, 4).order(ByteOrder.LITTLE_ENDIAN).getInt())); // 2691335425
			abc[_b] = abc[_b].plus(UnsignedInteger.fromIntBits(ByteBuffer.wrap(bytes, len + 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt()));
			abc[_c] = abc[_c].plus(UnsignedInteger.fromIntBits(ByteBuffer.wrap(bytes, len + 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt()));
			len += 12;
			abc = mix32(abc);
		}

		/* handle the last 11 bytes */
		if (len < bytes.length) {
			byte[] bytes1 = new byte[20];
			System.arraycopy(bytes, len, bytes1, 0, bytes.length - len);

			switch (bytes.length - len) {
				case (11):
					abc[_c] = abc[_c].plus(UnsignedInteger.fromIntBits((int) bytes[10 + len] << 24));
				case (10):
					abc[_c] = abc[_c].plus(UnsignedInteger.fromIntBits((int) bytes[9 + len] << 16));
				case (9):
					abc[_c] = abc[_c].plus(UnsignedInteger.fromIntBits((int) bytes[8 + len] << 8));
				case (8):
					abc[_a] = abc[_a].plus(UnsignedInteger.fromIntBits(ByteBuffer.wrap(bytes1, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt()));
					abc[_b] = abc[_b].plus(UnsignedInteger.fromIntBits(ByteBuffer.wrap(bytes1, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt()));
					break;
				case (7):
					abc[_b] = abc[_b].plus(UnsignedInteger.fromIntBits((int) bytes[6 + len] << 16));
				case (6):
					abc[_b] = abc[_b].plus(UnsignedInteger.fromIntBits((int) bytes[5 + len] << 8));
				case (5):
					abc[_b] = abc[_b].plus(UnsignedInteger.fromIntBits(bytes[4 + len]));
				case (4):
					abc[_a] = abc[_a].plus(UnsignedInteger.fromIntBits(ByteBuffer.wrap(bytes1, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt()));
					break;
				case (3):
					abc[_a] = abc[_a].plus(UnsignedInteger.fromIntBits((int) bytes[2 + len] << 16));
				case (2):
					abc[_a] = abc[_a].plus(UnsignedInteger.fromIntBits((int) bytes[1 + len] << 8));
				case 1:
					abc[_a] = abc[_a].plus(UnsignedInteger.fromIntBits(bytes[len]));
			}
		}

		UnsignedInteger[] bc = final32(abc);

		return bc[__b].longValue() << 32 | bc[__c].longValue();
	}

	public static long hashCombine64(long a, long b) {
		return a ^ (b + 0x49a0f4dd15e5a8e3L + (a << 54) + (a >>> 7));
	}

	public static long hashChar(byte c, long seed) {
		return hashInt(UnsignedInteger.fromIntBits(c), seed);
	}

	static public long hashBool(boolean val, long seed) {
		return hashInt(val ? UnsignedInteger.valueOf(1) : UnsignedInteger.valueOf(0), seed);
	}

	static public long hashInt2(short val, long seed) {
		return hashInt(UnsignedInteger.fromIntBits(val), seed);
	}

	static public long hashInt4(int val, long seed) {
		return hashInt(UnsignedInteger.fromIntBits(val), seed);
	}

	static public long hashInt8(long val, long seed) {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(val);

		UnsignedInteger hipart = UnsignedInteger.valueOf(val);
		UnsignedInteger lopart =  UnsignedInteger.valueOf(val);

		if (val >= 0) {
			lopart = xor(lopart, hipart);
		} else {
			lopart = xor(lopart, not(hipart));
		}
		return hashInt(lopart, seed);
	}

	public static long hashUuid(UUID uuid, long seed) {
		ByteBuffer bb = ByteBuffer.allocate(16);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return hashBytes(bb.array(), seed);
	}

	public static long hashFloat32(float val, long seed) {
		if (val == 0) {
			return seed;
		}

		double dval = val;
		if (Double.isNaN(dval)) {
			dval = Double.NaN;
		}
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(dval);
		return hashBytes(bb.array(), seed);
	}

	public static long hashFloat64(double val, long seed) {
		if (val == 0) {
			return seed;
		}

		if (Double.isNaN(val)) {
			val = Double.NaN;
		}
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(val);
		return hashBytes(bb.array(), seed);
	}

	private static UnsignedInteger xor(UnsignedInteger x, UnsignedInteger y) {
		return UnsignedInteger.valueOf(x.longValue() ^ y.longValue());
	}

	private static UnsignedInteger not(UnsignedInteger x) {
		return UnsignedInteger.valueOf(~x.longValue());
	}
}
