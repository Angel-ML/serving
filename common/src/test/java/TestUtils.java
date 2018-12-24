import com.tencent.angel.core.graph.TensorProtos;
import com.tencent.angel.utils.ProtoUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import com.tencent.angel.ml.math2.vector.Vector;

import java.util.*;

public class TestUtils {
    static ArrayList<Integer> keysInt = new ArrayList<Integer>();
    static ArrayList<Long> keysLong = new ArrayList<Long>();
    static ArrayList<Float> values = new ArrayList<Float>();

    @BeforeClass
    static public void createData() {
        keysInt.add(1);
        keysInt.add(2);
        keysInt.add(3);
        keysInt.add(4);
        keysInt.add(6);
        keysInt.add(8);
        keysInt.add(10);
        keysInt.add(17);
        keysInt.add(22);

        keysLong.add(1L);
        keysLong.add(2L);
        keysLong.add(5L);
        keysLong.add(8L);
        keysLong.add(11L);
        keysLong.add(13L);
        keysLong.add(19L);
        keysLong.add(22L);
        keysLong.add(26L);
        keysLong.add(30L);

        values.add(1.0f);
        values.add(2.0f);
        values.add(3.0f);
        values.add(5.0f);
        values.add(7.0f);
        values.add(11.0f);
        values.add(13.0f);
        values.add(17.0f);
        values.add(23.0f);
    }

    @Test
    public void totensorTest1() {
        TensorProtos.TensorProto tensor = ProtoUtils.toTensorProto(values);

        for (Float i : tensor.getFloatValList()) {
            System.out.println(i);
        }

        System.out.println(tensor.toString());
    }

    @Test
    public void totensorTest2() {
        TensorProtos.TensorProto tensor = ProtoUtils.toTensorProto(32L, keysLong, values);

        System.out.println(tensor.toString());
    }

    @Test
    public void totensorTest3() {
        TensorProtos.TensorProto tensor = ProtoUtils.toTensorProto(32L, keysInt, values);
        Vector vec = ProtoUtils.toVector(tensor);
    }
}
