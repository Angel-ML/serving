package com.tencent.angel.utils;

import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.serving.apis.common.TypesProtos.*;
import com.tencent.angel.serving.apis.common.InstanceProtos.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InstanceUtils {

    // for dense vector
    static public Vector getDenseVector(Instance instance) {
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
                int count = listValue.getICount();
                assert (dim == count);
                long[] data = new long[count];
                for (int i = 0; i < count; i++) {
                    data[i] = listValue.getL(i);
                }

                return VFactory.denseLongVector(data);
            }
            case DT_FLOAT: {
                int count = listValue.getICount();
                assert (dim == count);
                float[] data = new float[count];
                for (int i = 0; i < count; i++) {
                    data[i] = listValue.getF(i);
                }

                return VFactory.denseFloatVector(data);
            }
            case DT_DOUBLE: {
                int count = listValue.getICount();
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
    static public Vector getIntKeySparseVector(Instance instance) {
        int dim = (int) instance.getShape().getDim(0).getSize();
        MapValue mapValue = instance.getMv();

        DataType dType = instance.getDType();

        int idx = 0;
        switch (dType) {
            case DT_INT32: {
                Iterator<Map.Entry<Integer, Integer>> iter = mapValue.getI2IMapMap().entrySet().iterator();
                int count = mapValue.getI2BMapCount();
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
                int count = mapValue.getI2BMapCount();
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
                int count = mapValue.getI2BMapCount();
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
                int count = mapValue.getI2BMapCount();
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
    static public Vector getLongKeySparseVector(Instance instance) {
        long dim = instance.getShape().getDim(0).getSize();
        MapValue mapValue = instance.getMv();

        DataType dType = instance.getDType();

        int idx = 0;
        switch (dType) {
            case DT_INT32: {
                Iterator<Map.Entry<Long, Integer>> iter = mapValue.getL2IMapMap().entrySet().iterator();
                int count = mapValue.getI2BMapCount();
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
                int count = mapValue.getI2BMapCount();
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
                int count = mapValue.getI2BMapCount();
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
                int count = mapValue.getI2BMapCount();
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
        }

        return null;
    }

    // for dense matrix
    static public Matrix getBlasVector(Instance instance) {
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
                int count = listValue.getICount();
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
