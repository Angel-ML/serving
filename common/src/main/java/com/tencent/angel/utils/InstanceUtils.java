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

import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.serving.apis.common.TypesProtos.*;
import com.tencent.angel.serving.apis.common.InstanceProtos.*;

import java.util.Iterator;
import java.util.Map;

public class InstanceUtils {

    // for dense vector
    static private Vector getDenseVector(Instance instance) {
        long dim = instance.getShape().getDim(0).getSize();
        ListValue listValue = instance.getLv();

        DataType dType = instance.getDType();
        switch (dType) {
            case DT_INT32: {
                int count = listValue.getICount();
                assert (dim == count);
                int[] data = new int[count];
                for (int i = 0; i < count; i++) {
                    data[i] = listValue.getI(i);
                }

                return VFactory.denseIntVector(data);
            }
            case DT_INT64: {
                int count = listValue.getLCount();
                assert (dim == count);
                long[] data = new long[count];
                for (int i = 0; i < count; i++) {
                    data[i] = listValue.getL(i);
                }

                return VFactory.denseLongVector(data);
            }
            case DT_FLOAT: {
                int count = listValue.getFCount();
                assert (dim == count);
                float[] data = new float[count];
                for (int i = 0; i < count; i++) {
                    data[i] = listValue.getF(i);
                }

                return VFactory.denseFloatVector(data);
            }
            case DT_DOUBLE: {
                int count = listValue.getDCount();
                assert (dim == count);
                double[] data = new double[count];
                for (int i = 0; i < count; i++) {
                    data[i] = listValue.getD(i);
                }

                return VFactory.denseDoubleVector(data);
            }
        }

        return null;
    }

    // for int key sparse vector
    static private Vector getIntKeySparseVector(Instance instance) {
        int dim = (int) instance.getShape().getDim(0).getSize();
        MapValue mapValue = instance.getMv();

        DataType dType = instance.getDType();

        int idx = 0;
        switch (dType) {
            case DT_INT32: {
                Iterator<Map.Entry<Integer, Integer>> iter = mapValue.getI2IMapMap().entrySet().iterator();
                int count = mapValue.getI2IMapCount();
                int[] keys = new int[count];
                int[] data = new int[count];
                while (iter.hasNext()) {
                    Map.Entry<Integer, Integer> entry = iter.next();
                    keys[idx] = entry.getKey();
                    data[idx] = entry.getValue();
                    idx++;
                }
                return VFactory.sparseIntVector(dim, keys, data);
            }
            case DT_INT64: {
                Iterator<Map.Entry<Integer, Long>> iter = mapValue.getI2LMapMap().entrySet().iterator();
                int count = mapValue.getI2LMapCount();
                int[] keys = new int[count];
                long[] data = new long[count];
                while (iter.hasNext()) {
                    Map.Entry<Integer, Long> entry = iter.next();
                    keys[idx] = entry.getKey();
                    data[idx] = entry.getValue();
                    idx++;
                }
                return VFactory.sparseLongVector(dim, keys, data);
            }
            case DT_FLOAT: {
                Iterator<Map.Entry<Integer, Float>> iter = mapValue.getI2FMapMap().entrySet().iterator();
                int count = mapValue.getI2FMapCount();
                int[] keys = new int[count];
                float[] data = new float[count];
                while (iter.hasNext()) {
                    Map.Entry<Integer, Float> entry = iter.next();
                    keys[idx] = entry.getKey();
                    data[idx] = entry.getValue();
                    idx++;
                }
                return VFactory.sparseFloatVector(dim, keys, data);
            }
            case DT_DOUBLE: {
                Iterator<Map.Entry<Integer, Double>> iter = mapValue.getI2DMapMap().entrySet().iterator();
                int count = mapValue.getI2DMapCount();
                int[] keys = new int[count];
                double[] data = new double[count];
                while (iter.hasNext()) {
                    Map.Entry<Integer, Double> entry = iter.next();
                    keys[idx] = entry.getKey();
                    data[idx] = entry.getValue();
                    idx++;
                }
                return VFactory.sparseDoubleVector(dim, keys, data);
            }
        }

        return null;
    }

    // for long key sparse vector
    static private Vector getLongKeySparseVector(Instance instance) {
        long dim = instance.getShape().getDim(0).getSize();
        MapValue mapValue = instance.getMv();

        DataType dType = instance.getDType();

        int idx = 0;
        switch (dType) {
            case DT_INT32: {
                Iterator<Map.Entry<Long, Integer>> iter = mapValue.getL2IMapMap().entrySet().iterator();
                int count = mapValue.getI2IMapCount();
                long[] keys = new long[count];
                int[] data = new int[count];
                while (iter.hasNext()) {
                    Map.Entry<Long, Integer> entry = iter.next();
                    keys[idx] = entry.getKey();
                    data[idx] = entry.getValue();
                    idx++;
                }
                return VFactory.sparseLongKeyIntVector(dim, keys, data);
            }
            case DT_INT64: {
                Iterator<Map.Entry<Long, Long>> iter = mapValue.getL2LMapMap().entrySet().iterator();
                int count = mapValue.getI2LMapCount();
                long[] keys = new long[count];
                long[] data = new long[count];
                while (iter.hasNext()) {
                    Map.Entry<Long, Long> entry = iter.next();
                    keys[idx] = entry.getKey();
                    data[idx] = entry.getValue();
                    idx++;
                }
                return VFactory.sparseLongKeyLongVector(dim, keys, data);
            }
            case DT_FLOAT: {
                Iterator<Map.Entry<Long, Float>> iter = mapValue.getL2FMapMap().entrySet().iterator();
                int count = mapValue.getI2FMapCount();
                long[] keys = new long[count];
                float[] data = new float[count];
                while (iter.hasNext()) {
                    Map.Entry<Long, Float> entry = iter.next();
                    keys[idx] = entry.getKey();
                    data[idx] = entry.getValue();
                    idx++;
                }
                return VFactory.sparseLongKeyFloatVector(dim, keys, data);
            }
            case DT_DOUBLE: {
                Iterator<Map.Entry<Long, Double>> iter = mapValue.getL2DMapMap().entrySet().iterator();
                int count = mapValue.getI2DMapCount();
                long[] keys = new long[count];
                double[] data = new double[count];
                while (iter.hasNext()) {
                    Map.Entry<Long, Double> entry = iter.next();
                    keys[idx] = entry.getKey();
                    data[idx] = entry.getValue();
                    idx++;
                }
                return VFactory.sparseLongKeyDoubleVector(dim, keys, data);
            }
        }

        return null;
    }

    //for str
    static public String getStr(Instance instance) {
        return instance.getS();
    }

    static public Vector getVector(Instance instance) {
        switch (instance.getFlag()) {
            case IF_DENSE_VECTOR:
                return getDenseVector(instance);
            case IF_INTKEY_SPARSE_VECTOR:
                return getIntKeySparseVector(instance);
            case IF_LONGKEY_SPARSE_VECTOR:
                return getLongKeySparseVector(instance);
        }

        return null;
    }

    // for string key vector
    static public Map<String, ?> getStringKeyMap(Instance instance) {
        MapValue mapValue = instance.getMv();

        DataType dType = instance.getDType();
        switch (dType) {
            case DT_INT32:
                return mapValue.getS2IMapMap();
            case DT_INT64:
                return mapValue.getS2LMapMap();
            case DT_FLOAT:
                return mapValue.getS2FMapMap();
            case DT_DOUBLE:
                return mapValue.getS2DMapMap();
            case DT_STRING:
                Map<String, String> temp = mapValue.getS2SMapMap();
                if (temp != null) {
                    return temp;
                } else {
                    return mapValue.getS2BsMapMap();
                }
        }

        return null;
    }

    // for dense matrix
    static public Matrix getBlasMatrix(Instance instance) {
        int numRows = (int) instance.getShape().getDim(0).getSize();
        int numCols = (int) instance.getShape().getDim(1).getSize();
        ListValue listValue = instance.getLv();

        DataType dType = instance.getDType();
        switch (dType) {
            case DT_FLOAT: {
                int count = listValue.getICount();
                float[] data = new float[count];
                for (int i = 0; i < count; i++) {
                    data[i] = listValue.getF(i);
                }

                return MFactory.denseFloatMatrix(numRows, numCols, data);
            }
            case DT_DOUBLE: {
                int count = listValue.getDCount();
                double[] data = new double[count];
                for (int i = 0; i < count; i++) {
                    data[i] = listValue.getD(i);
                }

                return MFactory.denseDoubleMatrix(numRows, numCols, data);
            }
        }

        return null;
    }
}
