package com.mortardata.pig.collections;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;

public abstract class PigCollection {

    private static final Log log = LogFactory.getLog(PigCollection.class);

    public static final byte INT_ARRAY            = 1;
    public static final byte FLOAT_ARRAY         = 2;
    public static final byte INT_INT_MAP          = 3;
    public static final byte INT_FLOAT_MAP       = 4;
    public static final byte STRING_INT_MAP       = 5;
    public static final byte STRING_FLOAT_MAP    = 6;
    public static final byte INT_SET              = 7;
    public static final byte STRING_SET           = 8;

    public static class IntArray {
        public int[] array;
        public IntArray(int[] array) {
            this.array = array;
        }
    }

    public static class FloatArray {
        public float[] array;
        public FloatArray(float[] array) {
            this.array = array;
        }
    }

    // Map and Set types are serialized directly as objects
    // This is because you can test (obj instanceof TIntIntHashMap),
    // but there is no way to test (obj instanceof int[]), for example

    public static DataByteArray serialize(int[] arr) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(INT_ARRAY);
            out.writeInt(arr.length);
            for (int i = 0; i < arr.length; i++) {
                out.writeInt(arr[i]);
            }
            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataByteArray serialize(float[] arr) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(FLOAT_ARRAY);
            out.writeInt(arr.length);
            for (int i = 0; i < arr.length; i++) {
                out.writeFloat(arr[i]);
            };
            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataByteArray serialize(TIntIntHashMap map) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(INT_INT_MAP);
            out.writeInt(map.size());
            TIntIntIterator it = map.iterator();
            for (int i = 0; i < map.size(); i++) {
                it.advance();
                out.writeInt(it.key());
                out.writeInt(it.value());
            }
            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataByteArray serialize(TIntFloatHashMap map) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(INT_FLOAT_MAP);
            out.writeInt(map.size());
            TIntFloatIterator it = map.iterator();
            for (int i = 0; i < map.size(); i++) {
                it.advance();
                out.writeInt(it.key());
                out.writeFloat(it.value());
            }
            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataByteArray serialize(TObjectIntHashMap map) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(STRING_INT_MAP);
            out.writeInt(map.size());
            TObjectIntIterator it = map.iterator();
            for (int i = 0; i < map.size(); i++) {
                it.advance();
                out.writeUTF((String) it.key());
                out.writeInt(it.value());
            }
            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataByteArray serialize(TObjectFloatHashMap map) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(STRING_FLOAT_MAP);
            out.writeInt(map.size());
            TObjectFloatIterator it = map.iterator();
            for (int i = 0; i < map.size(); i++) {
                it.advance();
                out.writeUTF((String) it.key());
                out.writeFloat(it.value());
            }
            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataByteArray serialize(TIntHashSet set) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(INT_SET);
            out.writeInt(set.size());
            TIntIterator it = set.iterator();
            for (int i = 0; i < set.size(); i++) {
                out.writeInt(it.next());
            }
            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataByteArray serialize(Set<String> set) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(STRING_SET);
            out.writeInt(set.size());
            for (String str : set) {
                out.writeUTF(str);
            }
            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // if you know what you are expecting, you can deserialize directly using these helpers
    // or you can call the general method Object deserialize(DataByteArray dba)
    // and type introspect what that returns

    public static int[] deserializeIntArray(DataByteArray dba) { 
        return ((IntArray) deserialize(dba)).array;
    }

    public static float[] deserializeFloatArray(DataByteArray dba) {
        return ((FloatArray) deserialize(dba)).array;
    }

    public static TIntIntHashMap deserializeIntIntMap(DataByteArray dba) {
        return (TIntIntHashMap) deserialize(dba);
    }

    public static TIntFloatHashMap deserializeIntFloatMap(DataByteArray dba) {
        return (TIntFloatHashMap) deserialize(dba);
    }

    public static TObjectIntHashMap deserializeStringIntMap(DataByteArray dba) {
        return (TObjectIntHashMap) deserialize(dba);
    }

    public static TObjectFloatHashMap deserializeStringFloatMap(DataByteArray dba) {
        return (TObjectFloatHashMap) deserialize(dba);
    }

    public static TIntHashSet deserializeIntSet(DataByteArray dba) {
        return (TIntHashSet) deserialize(dba);
    }

    public static Set<String> deserializeStringSet(DataByteArray dba) {
        return (Set<String>) deserialize(dba);
    }

    public static Object deserialize(DataByteArray dba) {
        try {
            DataInput in = new DataInputStream(new ByteArrayInputStream(dba.get()));
            byte type = in.readByte();

            if (type == INT_ARRAY) {
                int len = in.readInt();
                int[] arr = new int[len];
                for (int i = 0; i < len; i++) {
                    arr[i] = in.readInt();
                }
                return new IntArray(arr);
            } else if (type == FLOAT_ARRAY) {
                int len = in.readInt();
                float[] arr = new float[len];
                for (int i = 0; i < len; i++) {
                    arr[i] = in.readFloat();
                }
                return new FloatArray(arr);
            } else if (type == INT_INT_MAP) {
                int len = in.readInt();
                TIntIntHashMap map = new TIntIntHashMap(len);
                for (int i = 0; i < len; i++) {
                    map.put(in.readInt(), in.readInt());
                }
                return map;
            } else if (type == INT_FLOAT_MAP) {
                int len = in.readInt();
                TIntFloatHashMap map = new TIntFloatHashMap(len);
                for (int i = 0; i < len; i++) {
                    map.put(in.readInt(), in.readFloat());
                }
                return map;
            } else if (type == STRING_INT_MAP) {
                int len = in.readInt();
                TObjectIntHashMap map = new TObjectIntHashMap(len);
                for (int i = 0; i < len; i++) {
                    map.put(in.readUTF(), in.readInt());
                }
                return map;
            } else if (type == STRING_FLOAT_MAP) {
                int len = in.readInt();
                TObjectFloatHashMap map = new TObjectFloatHashMap(len);
                for (int i = 0; i < len; i++) {
                    map.put(in.readUTF(), in.readFloat());
                }
                return map;
            } else if (type == INT_SET) {
                int len = in.readInt();
                TIntHashSet set = new TIntHashSet(len);
                for (int i = 0; i < len; i++) {
                    set.add(in.readInt());
                }
                return set;
            } else if (type == STRING_SET) {
                int len = in.readInt();
                Set<String> set = new HashSet(len);
                for (int i = 0; i < len; i++) {
                    set.add(in.readUTF());
                }
                return set;
            } else {
                throw new RuntimeException(
                    "Input DataByteArray does not seem" +
                    "to have been serialized by PigCollection"
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}