import com.tencent.angel.serving.apis.common.InstanceProtos;
import com.tencent.angel.utils.ProtoUtils;
import org.junit.BeforeClass;
import org.junit.Test;

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
    public void totensorTest1() throws Exception {
        InstanceProtos.Instance instance = ProtoUtils.getInstance(values);
        for (Float i : instance.getLv().getFList()) {
            System.out.println(i);
        }
        System.out.println(instance.toString());
    }

    @Test
    public void totensorTest2() throws Exception {
        Map<Long, Float> longFloatMap = new HashMap<>();
        for(int i = 0; i < keysLong.size(); i++) {
            longFloatMap.put(keysLong.get(i), values.get(i));
        }
        InstanceProtos.Instance instance = ProtoUtils.getInstance(32L, longFloatMap);
        System.out.println(instance.toString());
    }

    @Test
    public void totensorTest3() throws Exception {
        Map<Integer, Float> integerFloatHashMap = new HashMap<>();
        for(int i = 0; i < keysInt.size(); i++) {
            integerFloatHashMap.put(keysInt.get(i), values.get(i));
        }
        InstanceProtos.Instance instance = ProtoUtils.getInstance(32, integerFloatHashMap);
        System.out.println(instance.toString());
    }
}