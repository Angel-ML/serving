/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.tencent.angel.serving.apis.common.TensorShapeProtos.*;
import com.tencent.angel.serving.apis.common.TypesProtos.DataType;
import com.tencent.angel.serving.apis.common.ModelSpecProtos.*;
import com.tencent.angel.serving.apis.common.InstanceProtos.*;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;


public class ProtoUtils {

    static public Int64Value getVersion(Long modelVersion) {
        return Int64Value.newBuilder().setValue(modelVersion).build();
    }

    static public TensorShapeProto getShape(Long... dims) {
        TensorShapeProto.Builder builder = TensorShapeProto.newBuilder();

        for (long dim : dims) {
            TensorShapeProto.Dim.Builder dimBuilder = TensorShapeProto.Dim.newBuilder();
            dimBuilder.setSize(dim);
            builder.addDim(dimBuilder.build());
        }

        return builder.build();
    }

    static public TensorShapeProto getShape(String[] names, Long[] dims) {
        TensorShapeProto.Builder builder = TensorShapeProto.newBuilder();

        for (int i = 0; i < dims.length; i++) {
            TensorShapeProto.Dim.Builder dimBuilder = TensorShapeProto.Dim.newBuilder();
            dimBuilder.setName(names[i]);
            dimBuilder.setSize(dims[i]);
            builder.addDim(dimBuilder.build());
        }

        return builder.build();
    }

    static public ModelSpec getModelSpec(String modelName, Long version, String signature) {
        return ModelSpec.newBuilder()
                .setName(modelName)
                .setVersion(getVersion(version))
                .setSignatureName(signature)
                .build();
    }

    // 0 D
    static public <T> Instance getInstance(T value) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        instanceBuilder.setShape(getShape(0L));
        if (value instanceof Boolean) {
            instanceBuilder.setDType(DataType.DT_BOOL);
            instanceBuilder.setB((Boolean) value);
        } else if (value instanceof Integer) {
            instanceBuilder.setDType(DataType.DT_INT32);
            instanceBuilder.setI((Integer) value);
        } else if (value instanceof Long) {
            instanceBuilder.setDType(DataType.DT_INT64);
            instanceBuilder.setL((Long) value);
        } else if (value instanceof Float) {
            instanceBuilder.setDType(DataType.DT_FLOAT);
            instanceBuilder.setF((Float) value);
        } else if (value instanceof Double) {
            instanceBuilder.setDType(DataType.DT_DOUBLE);
            instanceBuilder.setD((Double) value);
        } else if (value instanceof String) {
            instanceBuilder.setDType(DataType.DT_STRING);
            instanceBuilder.setS((String) value);
        } else if (value instanceof ByteString) {
            instanceBuilder.setDType(DataType.DT_STRING);
            instanceBuilder.setBs((ByteString) value);
        } else {
            throw new Exception("unsuported data type!");
        }

        return instanceBuilder.build();
    }

    // named 0 D
    static public <T> Instance getInstance(String name, T value) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        instanceBuilder.setShape(getShape(0L));
        instanceBuilder.setName(name);
        if (value instanceof Boolean) {
            instanceBuilder.setDType(DataType.DT_BOOL);
            instanceBuilder.setB((Boolean) value);
        } else if (value instanceof Integer) {
            instanceBuilder.setDType(DataType.DT_INT32);
            instanceBuilder.setI((Integer) value);
        } else if (value instanceof Long) {
            instanceBuilder.setDType(DataType.DT_INT64);
            instanceBuilder.setL((Long) value);
        } else if (value instanceof Float) {
            instanceBuilder.setDType(DataType.DT_FLOAT);
            instanceBuilder.setF((Float) value);
        } else if (value instanceof Double) {
            instanceBuilder.setDType(DataType.DT_DOUBLE);
            instanceBuilder.setD((Double) value);
        } else if (value instanceof ByteString) {
            instanceBuilder.setDType(DataType.DT_STRING);
            instanceBuilder.setBs((ByteString) value);
        } else if (value instanceof String) {
            instanceBuilder.setDType(DataType.DT_STRING);
            instanceBuilder.setS((String) value);
        } else {
            throw new Exception("unsuported data type!");
        }

        return instanceBuilder.build();
    }

    static private <T> Tuple2<Integer, Integer> addElements(ListValue.Builder lvBuilder, Iterator<T> values) throws Exception {
        int dim = 0;
        int dtype = -1;
        if (values.hasNext()) {
            T value = values.next();
            if (value instanceof Boolean) {
                dtype = 1;
                lvBuilder.addB((Boolean) value);
            } else if (value instanceof Integer) {
                dtype = 2;
                lvBuilder.addI((Integer) value);
            } else if (value instanceof Long) {
                dtype = 3;
                lvBuilder.addL((Long) value);
            } else if (value instanceof Float) {
                dtype = 4;
                lvBuilder.addF((Float) value);
            } else if (value instanceof Double) {
                dtype = 5;
                lvBuilder.addD((Double) value);
            } else if (value instanceof String) {
                dtype = 6;
                lvBuilder.addS((String) value);
            } else if (value instanceof ByteString) {
                dtype = 7;
                lvBuilder.addBs((ByteString) value);
            } else {
                throw new Exception("unsuported data type!");
            }

            dim++;
        }

        switch (dtype) {
            case 1:
                while (values.hasNext()) {
                    T value = values.next();
                    lvBuilder.addB((Boolean) value);
                    dim++;
                }
            case 2:
                while (values.hasNext()) {
                    T value = values.next();
                    lvBuilder.addI((Integer) value);
                    dim++;
                }
                break;
            case 3:
                while (values.hasNext()) {
                    T value = values.next();
                    lvBuilder.addL((Long) value);
                    dim++;
                }
                break;
            case 4:
                while (values.hasNext()) {
                    T value = values.next();
                    lvBuilder.addF((Float) value);
                    dim++;
                }
                break;
            case 5:
                while (values.hasNext()) {
                    T value = values.next();
                    lvBuilder.addD((Double) value);
                    dim++;
                }
                break;
            case 6:
                while (values.hasNext()) {
                    T value = values.next();
                    lvBuilder.addS((String) value);
                    dim++;
                }
                break;
            case 7:
                while (values.hasNext()) {
                    T value = values.next();
                    lvBuilder.addBs((ByteString) value);
                    dim++;
                }
                break;
        }

        return new Tuple2<Integer, Integer>(dim, dtype);
    }

    static private <K, V> int addElements(MapValue.Builder mvBuilder, Map<K, V> values) throws Exception {
        int dtype = -1;
        boolean multipleType = false;
        Iterator<Map.Entry<K, V>> iter = values.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
            K key = entry.getKey();
            V value = entry.getValue();
            if (key instanceof Integer && value instanceof Boolean) {
                dtype = 11;
                mvBuilder.putI2BMap((Integer) key, (Boolean) value);
            } else if (key instanceof Integer && value instanceof Integer) {
                dtype = 12;
                mvBuilder.putI2IMap((Integer) key, (Integer) value);
            } else if (key instanceof Integer && value instanceof Long) {
                dtype = 13;
                mvBuilder.putI2LMap((Integer) key, (Long) value);
            } else if (key instanceof Integer && value instanceof Float) {

                dtype = 14;
                mvBuilder.putI2FMap((Integer) key, (Float) value);
            } else if (key instanceof Integer && value instanceof Double) {
                dtype = 15;
                mvBuilder.putI2DMap((Integer) key, (Double) value);
            } else if (key instanceof Integer && value instanceof String) {
                dtype = 16;
                mvBuilder.putI2SMap((Integer) key, (String) value);
            } else if (key instanceof Integer && value instanceof ByteString) {
                dtype = 17;
                mvBuilder.putI2BsMap((Integer) key, (ByteString) value);
            } else if (key instanceof Long && value instanceof Boolean) {
                dtype = 21;
                mvBuilder.putL2BMap((Long) key, (Boolean) value);
            } else if (key instanceof Long && value instanceof Integer) {
                dtype = 22;
                mvBuilder.putL2IMap((Long) key, (Integer) value);
            } else if (key instanceof Long && value instanceof Long) {
                dtype = 23;
                mvBuilder.putL2LMap((Long) key, (Long) value);
            } else if (key instanceof Long && value instanceof Float) {
                dtype = 24;
                mvBuilder.putL2FMap((Long) key, (Float) value);
            } else if (key instanceof Long && value instanceof Double) {
                dtype = 25;
                mvBuilder.putL2DMap((Long) key, (Double) value);
            } else if (key instanceof Long && value instanceof String) {
                dtype = 26;
                mvBuilder.putL2SMap((Long) key, (String) value);
            } else if (key instanceof Long && value instanceof ByteString) {
                dtype = 27;
                mvBuilder.putL2BsMap((Long) key, (ByteString) value);
            } else if (key instanceof String && value instanceof Boolean) {
                if (dtype != -1 && dtype != 31) {
                    multipleType = true;
                }
                dtype = 31;
                mvBuilder.putS2BMap((String) key, (Boolean) value);
            } else if (key instanceof String && value instanceof Integer) {
                if (dtype != -1 && dtype != 32) {
                    multipleType = true;
                }
                dtype = 32;
                mvBuilder.putS2IMap((String) key, (Integer) value);
            } else if (key instanceof String && value instanceof Long) {
                if (dtype != -1 && dtype != 33) {
                    multipleType = true;
                }
                dtype = 33;
                mvBuilder.putS2LMap((String) key, (Long) value);
            } else if (key instanceof String && value instanceof Float) {
                if (dtype != -1 && dtype != 34) {
                    multipleType = true;
                }
                dtype = 34;
                mvBuilder.putS2FMap((String) key, (Float) value);
            } else if (key instanceof String && value instanceof Double) {
                if (dtype != -1 && dtype != 35) {
                    multipleType = true;
                }
                dtype = 35;
                mvBuilder.putS2DMap((String) key, (Double) value);
            } else if (key instanceof String && value instanceof String) {
                if (dtype != -1 && dtype != 36) {
                    multipleType = true;
                }
                dtype = 36;
                mvBuilder.putS2SMap((String) key, (String) value);
            } else if (key instanceof String && value instanceof ByteString) {
                if (dtype != -1 && dtype != 36) {
                    multipleType = true;
                }
                dtype = 36;
                mvBuilder.putS2BsMap((String) key, (ByteString) value);
            } else {
                throw new Exception("unsuported data type!");
            }
        }
        if (multipleType == true) {
            return -2;
        }


        switch (dtype) {
            case 11:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putI2BMap((Integer) key, (Boolean) value);
                }
                break;
            case 12:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putI2IMap((Integer) key, (Integer) value);
                }
                break;
            case 13:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putI2LMap((Integer) key, (Long) value);
                }
                break;
            case 14:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putI2FMap((Integer) key, (Float) value);
                }
                break;
            case 15:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putI2DMap((Integer) key, (Double) value);
                }
                break;
            case 16:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putI2SMap((Integer) key, (String) value);
                }
                break;
            case 17:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putI2BsMap((Integer) key, (ByteString) value);
                }
                break;
            case 21:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putL2BMap((Long) key, (Boolean) value);
                }
                break;
            case 22:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putL2IMap((Long) key, (Integer) value);
                }
                break;
            case 23:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putL2LMap((Long) key, (Long) value);
                }
                break;
            case 24:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putL2FMap((Long) key, (Float) value);
                }
                break;
            case 25:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putL2DMap((Long) key, (Double) value);
                }
                break;
            case 26:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putL2SMap((Long) key, (String) value);
                }
                break;
            case 27:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putL2BsMap((Long) key, (ByteString) value);
                }
                break;
            case 31:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putS2BMap((String) key, (Boolean) value);
                }
                break;
            case 32:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putS2IMap((String) key, (Integer) value);
                }
                break;
            case 33:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putS2LMap((String) key, (Long) value);
                }
                break;
            case 34:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putS2FMap((String) key, (Float) value);
                }
                break;
            case 35:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putS2DMap((String) key, (Double) value);
                }
                break;
            case 36:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putS2SMap((String) key, (String) value);
                }
                break;
            case 37:
                while (iter.hasNext()) {
                    Map.Entry<K, V> entry = iter.next();
                    K key = entry.getKey();
                    V value = entry.getValue();
                    mvBuilder.putS2BsMap((String) key, (ByteString) value);
                }
                break;


        }


        return dtype % 10;
    }

    static private void setDType(Instance.Builder instanceBuilder, int dtype) throws Exception {
        switch (dtype) {
            case -2:
                instanceBuilder.setDType(DataType.DT_MULTI_TYPE);
                break;
            case 1:
                instanceBuilder.setDType(DataType.DT_BOOL);
                break;
            case 2:
                instanceBuilder.setDType(DataType.DT_INT32);
                break;
            case 3:
                instanceBuilder.setDType(DataType.DT_INT64);
                break;
            case 4:
                instanceBuilder.setDType(DataType.DT_FLOAT);
                break;
            case 5:
                instanceBuilder.setDType(DataType.DT_DOUBLE);
                break;
            case 6:
                instanceBuilder.setDType(DataType.DT_STRING);
                break;
            case 7:
                instanceBuilder.setDType(DataType.DT_STRING);
                break;
            default:
                throw new Exception("unsuported data type!");
        }
    }

    // dense 1 D
    static public <T> Instance getInstance(Iterator<T> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        ListValue.Builder lvBuilder = ListValue.newBuilder();
        Tuple2<Integer, Integer> tuple = addElements(lvBuilder, values);
        instanceBuilder.setLv(lvBuilder.build());
        instanceBuilder.setFlag(InstanceFlag.IF_DENSE_VECTOR);

        instanceBuilder.setShape(getShape(Long.valueOf(tuple._1)));
        setDType(instanceBuilder, tuple._2);
        return instanceBuilder.build();
    }

    // dense named 1 D
    static public <T> Instance getInstance(String name, Iterator<T> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        ListValue.Builder lvBuilder = ListValue.newBuilder();
        Tuple2<Integer, Integer> tuple = addElements(lvBuilder, values);
        instanceBuilder.setLv(lvBuilder.build());
        instanceBuilder.setFlag(InstanceFlag.IF_DENSE_VECTOR);

        instanceBuilder.setShape(getShape(Long.valueOf(tuple._1)));
        setDType(instanceBuilder, tuple._2);
        instanceBuilder.setName(name);
        return instanceBuilder.build();
    }

    // sparse string key 1 D
    static public <V> Instance getInstance(Map<String, V> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        MapValue.Builder mvBuilder = MapValue.newBuilder();
        int dtype = addElements(mvBuilder, values);

        setDType(instanceBuilder, dtype);
        instanceBuilder.setMv(mvBuilder.build());
        instanceBuilder.setFlag(InstanceFlag.IF_STRINGKEY_VECTOR);
        return instanceBuilder.build();
    }

    // sparse string key named 1 D
    static public <V> Instance getInstance(String name, Map<String, V> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        instanceBuilder.setName(name);

        MapValue.Builder mvBuilder = MapValue.newBuilder();
        int dtype = addElements(mvBuilder, values);

        setDType(instanceBuilder, dtype);
        instanceBuilder.setMv(mvBuilder.build());
        instanceBuilder.setFlag(InstanceFlag.IF_STRINGKEY_VECTOR);
        return instanceBuilder.build();
    }

    // sparse int key 1 D
    static public <V> Instance getInstance(int dim, Map<Integer, V> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        instanceBuilder.setShape(getShape(Long.valueOf(dim)));

        MapValue.Builder mvBuilder = MapValue.newBuilder();
        int dtype = addElements(mvBuilder, values);

        setDType(instanceBuilder, dtype);
        instanceBuilder.setMv(mvBuilder.build());
        instanceBuilder.setFlag(InstanceFlag.IF_INTKEY_SPARSE_VECTOR);
        return instanceBuilder.build();
    }

    // sparse int key named 1 D
    static public <V> Instance getInstance(String name, int dim, Map<Integer, V> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        instanceBuilder.setName(name);
        instanceBuilder.setShape(getShape(Long.valueOf(dim)));

        MapValue.Builder mvBuilder = MapValue.newBuilder();
        int dtype = addElements(mvBuilder, values);

        setDType(instanceBuilder, dtype);
        instanceBuilder.setMv(mvBuilder.build());
        instanceBuilder.setFlag(InstanceFlag.IF_INTKEY_SPARSE_VECTOR);
        return instanceBuilder.build();
    }

    // sparse long key 1 D
    static public <V> Instance getInstance(long dim, Map<Long, V> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        instanceBuilder.setShape(getShape(dim));

        MapValue.Builder mvBuilder = MapValue.newBuilder();
        int dtype = addElements(mvBuilder, values);

        setDType(instanceBuilder, dtype);
        instanceBuilder.setMv(mvBuilder.build());
        instanceBuilder.setFlag(InstanceFlag.IF_LONGKEY_SPARSE_VECTOR);
        return instanceBuilder.build();
    }

    // sparse long key named 1 D
    static public <V> Instance getInstance(String name, long dim, Map<Long, V> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();

        instanceBuilder.setShape(getShape(dim));
        instanceBuilder.setName(name);

        MapValue.Builder mvBuilder = MapValue.newBuilder();
        int dtype = addElements(mvBuilder, values);

        setDType(instanceBuilder, dtype);
        instanceBuilder.setMv(mvBuilder.build());
        instanceBuilder.setFlag(InstanceFlag.IF_LONGKEY_SPARSE_VECTOR);
        return instanceBuilder.build();
    }

    // dense 2 D
    static public <T> Instance getInstance(int numRows, int numCols, Iterator<T> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();
        instanceBuilder.setShape(getShape(Long.valueOf(numRows), Long.valueOf(numCols)));

        ListValue.Builder lvBuilder = ListValue.newBuilder();
        Tuple2<Integer, Integer> tuple = addElements(lvBuilder, values);
        instanceBuilder.setLv(lvBuilder.build());

        setDType(instanceBuilder, tuple._2);
        instanceBuilder.setFlag(InstanceFlag.IF_2D_MATRIX);
        return instanceBuilder.build();
    }

    // named dense 2 D
    static public <T> Instance getInstance(String name, int numRows, int numCols, Iterator<T> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();
        instanceBuilder.setShape(getShape(Long.valueOf(numRows), Long.valueOf(numCols)));
        instanceBuilder.setName(name);

        ListValue.Builder lvBuilder = ListValue.newBuilder();
        Tuple2<Integer, Integer> tuple = addElements(lvBuilder, values);
        instanceBuilder.setLv(lvBuilder.build());

        setDType(instanceBuilder, tuple._2);
        instanceBuilder.setFlag(InstanceFlag.IF_2D_MATRIX);
        return instanceBuilder.build();
    }

    // dense 3 D
    static public <T> Instance getInstance(int numRows, int numCols, int numChannel, Iterator<T> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();
        instanceBuilder.setShape(getShape((long) numRows, (long) numCols, (long) numChannel));

        ListValue.Builder lvBuilder = ListValue.newBuilder();
        Tuple2<Integer, Integer> tuple = addElements(lvBuilder, values);
        instanceBuilder.setLv(lvBuilder.build());

        setDType(instanceBuilder, tuple._2);
        instanceBuilder.setFlag(InstanceFlag.IF_3D_MATRIX);
        return instanceBuilder.build();
    }

    // named dense 3 D
    static public <T> Instance getInstance(String name, int numRows, int numCols, int numChannel, Iterator<T> values) throws Exception {
        Instance.Builder instanceBuilder = Instance.newBuilder();
        instanceBuilder.setShape(getShape((long) numRows, (long) numCols, (long) numChannel));
        instanceBuilder.setName(name);

        ListValue.Builder lvBuilder = ListValue.newBuilder();
        Tuple2<Integer, Integer> tuple = addElements(lvBuilder, values);
        instanceBuilder.setLv(lvBuilder.build());

        setDType(instanceBuilder, tuple._2);
        instanceBuilder.setFlag(InstanceFlag.IF_3D_MATRIX);
        return instanceBuilder.build();
    }
}
