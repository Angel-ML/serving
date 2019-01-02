package com.tencent.angel.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.tencent.angel.core.graph.TensorProtos.TensorProto;
import com.tencent.angel.core.graph.TensorShapeProtos.TensorShapeProto;
import com.tencent.angel.core.graph.TypesProtos.DataType;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.serving.apis.common.ModelSpecProtos;


public class ProtoUtils {

    static public TensorShapeProto toShape(Long... dims) {
        TensorShapeProto.Builder builder = TensorShapeProto.newBuilder();
        for (long dim : dims) {
            TensorShapeProto.Dim tdim = TensorShapeProto.Dim.newBuilder().setSize(dim).build();
            builder.addDim(tdim);
        }
        return builder.build();
    }

    static public <T> TensorProto toTensorProto(ArrayList<T> data) {
        TensorProto.Builder builder = TensorProto.newBuilder();

        builder.setTensorShape(toShape((long) data.size()));
        String dataClassName = data.get(0).getClass().getSimpleName();

        switch (dataClassName) {
            case "Double":
                builder.setDtype(DataType.DT_DOUBLE);
                for (T d : data) {
                    builder.addDoubleVal((Double) d);
                }
                break;
            case "Float":
                builder.setDtype(DataType.DT_FLOAT);
                for (T d : data) {
                    builder.addFloatVal((Float) d);
                }
                break;
            case "Int":
                builder.setDtype(DataType.DT_INT32);
                for (T d : data) {
                    builder.addIntVal((Integer) d);
                }
                break;
            case "Long":
                builder.setDtype(DataType.DT_INT64);
                for (T d : data) {
                    builder.addInt64Val((Long) d);
                }
                break;
        }

        return builder.build();
    }

    static private int getTypeSize(String dType) {
        switch (dType) {
            case "Integer":
                return Integer.SIZE / 8;
            case "Long":
                return Long.SIZE / 8;
            case "Float":
                return Float.SIZE / 8;
            case "Double":
                return Double.SIZE / 8;
        }
        return -1;
    }

    static public <K, V> TensorProto toTensorProto(Long dim, List<K> keys, List<V> values) {
        TensorProto.Builder builder = TensorProto.newBuilder();
        builder.setTensorShape(toShape(dim));

        String keyType = keys.get(0).getClass().getSimpleName();
        String valueType = values.get(0).getClass().getSimpleName();

        int keySize = getTypeSize(keyType) * keys.size();
        int valueSize = getTypeSize(valueType) * values.size();

        int capacity = keySize + valueSize + 3 * getTypeSize("Integer");
        ByteBuffer buf = ByteBuffer.allocate(capacity);


        buf.putInt(keys.size());
        buf.putInt(getTypeSize(keyType));
        switch (keyType) {
            case "Integer":
                for (K d : keys) {
                    buf.putInt((Integer) d);
                }
                break;
            case "Long":
                for (K d : keys) {
                    buf.putLong((Long) d);
                }
                break;
        }

        buf.putInt(values.size());
        switch (valueType) {
            case "Double":
                builder.setDtype(DataType.DT_DOUBLE);
                for (V d : values) {
                    buf.putDouble((Double) d);
                }
                break;
            case "Float":
                builder.setDtype(DataType.DT_FLOAT);
                for (V d : values) {
                    buf.putFloat((Float) d);
                }
                break;
            case "Integer":
                builder.setDtype(DataType.DT_INT32);
                for (V d : values) {
                    buf.putInt((Integer) d);
                }
                break;
            case "Long":
                builder.setDtype(DataType.DT_INT64);
                for (V d : values) {
                    buf.putLong((Long) d);
                }
                break;
        }


        ByteString bstring = ByteString.copyFrom(buf.array());
        builder.setTensorContent(bstring);

        return builder.build();
    }

    static public Vector toVector(TensorProto tensor) {
        Vector res = null;

        DataType dType = tensor.getDtype();
        long dim = tensor.getTensorShape().getDim(0).getSize();
        ByteString content = tensor.getTensorContent();

        int i = 0;
        if (content != null && !content.isEmpty()) {
            ByteBuffer buf = content.asReadOnlyByteBuffer();
            int keySize = buf.getInt();
            int keyType = buf.getInt();
            int valueSize;
            if (keyType == getTypeSize("Integer")) {
                int[] keys = new int[keySize];
                while (i < keySize) {
                    keys[i] = buf.getInt();
                    i++;
                }

                i = 0;
                valueSize = buf.getInt();
                assert (keySize == valueSize);
                switch (dType) {
                    case DT_DOUBLE: {
                        double[] values = new double[valueSize];
                        while (i < valueSize) {
                            values[i] = buf.getDouble();
                            i++;
                        }
                        res = VFactory.sparseDoubleVector((int) dim, keys, values);
                        break;
                    }
                    case DT_FLOAT: {
                        float[] values = new float[valueSize];
                        while (i < valueSize) {
                            values[i] = buf.getFloat();
                            i++;
                        }
                        res = VFactory.sparseFloatVector((int) dim, keys, values);
                        break;
                    }
                    case DT_INT32: {
                        int[] values = new int[valueSize];
                        while (i < valueSize) {
                            values[i] = buf.getInt();
                            i++;
                        }
                        res = VFactory.sparseIntVector((int) dim, keys, values);
                        break;
                    }
                    case DT_INT64: {
                        long[] values = new long[valueSize];
                        while (i < valueSize) {
                            values[i] = buf.getLong();
                            i++;
                        }
                        res = VFactory.sparseLongVector((int) dim, keys, values);
                        break;
                    }
                }
            } else {
                long[] keys = new long[keySize];
                while (i < keySize) {
                    keys[i] = buf.getLong();
                    i++;
                }

                i = 0;
                valueSize = buf.getInt();
                assert (keySize == valueSize);
                switch (dType) {
                    case DT_DOUBLE: {
                        double[] values = new double[valueSize];
                        while (i < valueSize) {
                            values[i] = buf.getDouble();
                            i++;
                        }
                        res = VFactory.sparseLongKeyDoubleVector(dim, keys, values);
                        break;
                    }
                    case DT_FLOAT: {
                        float[] values = new float[valueSize];
                        while (i < valueSize) {
                            values[i] = buf.getFloat();
                            i++;
                        }
                        res = VFactory.sparseLongKeyFloatVector(dim, keys, values);
                        break;
                    }
                    case DT_INT32: {
                        int[] values = new int[valueSize];
                        while (i < valueSize) {
                            values[i] = buf.getInt();
                            i++;
                        }
                        res = VFactory.sparseLongKeyIntVector(dim, keys, values);
                        break;
                    }
                    case DT_INT64: {
                        long[] values = new long[valueSize];
                        while (i < valueSize) {
                            values[i] = buf.getLong();
                            i++;
                        }
                        res = VFactory.sparseLongKeyLongVector(dim, keys, values);
                        break;
                    }
                }
            }

        } else {
            int size = (int) dim;
            switch (dType) {
                case DT_DOUBLE: {
                    double[] values = new double[size];
                    while (i < size) {
                        values[i] = tensor.getDoubleVal(i);
                        i++;
                    }
                    res = VFactory.denseDoubleVector(values);
                    break;
                }
                case DT_FLOAT: {
                    float[] values = new float[size];
                    while (i < size) {
                        values[i] = tensor.getFloatVal(i);
                        i++;
                    }
                    res = VFactory.denseFloatVector(values);
                    break;
                }
                case DT_INT32: {
                    int[] values = new int[size];
                    while (i < size) {
                        values[i] = tensor.getIntVal(i);
                        i++;
                    }
                    res = VFactory.denseIntVector(values);
                    break;
                }
                case DT_INT64: {
                    long[] values = new long[size];
                    while (i < size) {
                        values[i] = tensor.getInt64Val(i);
                        i++;
                    }
                    res = VFactory.denseLongVector(values);
                    break;
                }
            }
        }

        return res;
    }

    static public Int64Value toVersion(Long modelVersion) {
        return Int64Value.newBuilder().setValue(modelVersion).build();
    }

    static public ModelSpecProtos.ModelSpec getModelSpec(String modelName, Long version, String signature) {
        return ModelSpecProtos.ModelSpec.newBuilder()
                .setName(modelName)
                .setVersion(toVersion(version))
                .setSignatureName(signature)
                .build();
    }

}
